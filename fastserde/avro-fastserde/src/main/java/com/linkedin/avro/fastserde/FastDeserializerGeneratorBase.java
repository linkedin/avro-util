package com.linkedin.avro.fastserde;

import com.sun.codemodel.JBlock;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.ListIterator;
import org.apache.avro.Schema;
import com.linkedin.avro.fastserde.backport.Symbol;
import org.apache.avro.util.Utf8;


/** TODO all of this could be moved to {@link FastDeserializerGenerator} */
public abstract class FastDeserializerGeneratorBase<T> extends FastSerdeBase {
  protected static final Symbol EMPTY_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {};
  protected static final Symbol END_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {};
  protected final Schema writer;
  protected final Schema reader;

  FastDeserializerGeneratorBase(boolean useGenericTypes, Schema writer, Schema reader, File destination, ClassLoader classLoader,
      String compileClassPath) {
    super("deserialization", useGenericTypes, Utf8.class, destination, classLoader, compileClassPath, false);
    this.writer = writer;
    this.reader = reader;
  }

  protected static Symbol[] reverseSymbolArray(Symbol[] symbols) {
    Symbol[] reversedSymbols = new Symbol[symbols.length];

    for (int i = 0; i < symbols.length; i++) {
      reversedSymbols[symbols.length - i - 1] = symbols[i];
    }

    return reversedSymbols;
  }

  public static String getClassName(Schema writerSchema, Schema readerSchema, String description) {
    Long writerSchemaId = Math.abs(Utils.getSchemaFingerprint(writerSchema));
    Long readerSchemaId = Math.abs(Utils.getSchemaFingerprint(readerSchema));
    String typeName = SchemaAssistant.getTypeName(readerSchema);
    return typeName + SEP + description + "Deserializer" + SEP + writerSchemaId + SEP + readerSchemaId;
  }

  protected static String getSymbolPrintName(Symbol symbol) {
    String printName;
    try {
      Field field = symbol.getClass().getDeclaredField("printName");

      field.setAccessible(true);
      printName = (String) field.get(symbol);
      field.setAccessible(false);
    } catch (ReflectiveOperationException e) {
      throw new FastDeserializerGeneratorException(e);
    }

    return printName;
  }

  protected static void assignBlockToBody(Object codeContainer, JBlock body) {
    try {
      Field field = codeContainer.getClass().getDeclaredField("body");

      field.setAccessible(true);
      field.set(codeContainer, body);
      field.setAccessible(false);
    } catch (ReflectiveOperationException e) {
      throw new FastDeserializerGeneratorException(e);
    }
  }

  public abstract FastDeserializer<T> generateDeserializer();

  protected ListIterator<Symbol> actionIterator(FieldAction action) {
    ListIterator<Symbol> actionIterator = null;

    if (action.getSymbolIterator() != null) {
      actionIterator = action.getSymbolIterator();
    } else if (action.getSymbol().production != null) {
      actionIterator = Arrays.asList(reverseSymbolArray(action.getSymbol().production)).listIterator();
    } else {
      actionIterator = Collections.emptyListIterator();
    }

    while (actionIterator.hasNext()) {
      Symbol symbol = actionIterator.next();

      if (symbol instanceof Symbol.ErrorAction) {
        throw new FastDeserializerGeneratorException(((Symbol.ErrorAction) symbol).msg);
      }

      if (symbol instanceof Symbol.FieldOrderAction) {
        break;
      }
    }

    return actionIterator;
  }

  protected void forwardToExpectedDefault(ListIterator<Symbol> symbolIterator) {
    Symbol symbol;
    while (symbolIterator.hasNext()) {
      symbol = symbolIterator.next();

      if (symbol instanceof Symbol.ErrorAction) {
        throw new FastDeserializerGeneratorException(((Symbol.ErrorAction) symbol).msg);
      }

      if (symbol instanceof Symbol.DefaultStartAction) {
        return;
      }
    }
    throw new FastDeserializerGeneratorException("DefaultStartAction symbol expected!");
  }

  protected FieldAction seekFieldAction(boolean shouldReadCurrent, Schema.Field field,
      ListIterator<Symbol> symbolIterator) {

    Schema.Type type = field.schema().getType();

    if (!shouldReadCurrent) {
      return FieldAction.fromValues(type, false, EMPTY_SYMBOL);
    }

    boolean shouldRead = true;
    Symbol fieldSymbol = END_SYMBOL;

    if (Schema.Type.RECORD.equals(type)) {
      if (symbolIterator.hasNext()) {
        fieldSymbol = symbolIterator.next();
        if (fieldSymbol instanceof Symbol.SkipAction) {
          return FieldAction.fromValues(type, false, fieldSymbol);
        } else {
          symbolIterator.previous();
        }
      }
      return FieldAction.fromValues(type, true, symbolIterator);
    }

    while (symbolIterator.hasNext()) {
      Symbol symbol = symbolIterator.next();
/*
      if (Symbol.Kind.REPEATER.equals(symbol.kind)
          && "array-end"
          .equals(
              getSymbolPrintName(
                  ((Symbol.Repeater) symbol)
                      .end))) {
        symbolIterator = Arrays.asList(reverseSymbolArray(symbol.production)).listIterator();
        break;
      }
*/

      if (symbol instanceof Symbol.ErrorAction) {
        throw new FastDeserializerGeneratorException(((Symbol.ErrorAction) symbol).msg);
      }

      if (symbol instanceof Symbol.SkipAction) {
        shouldRead = false;
        fieldSymbol = symbol;
        break;
      }

      if (symbol instanceof Symbol.WriterUnionAction) {
        if (symbolIterator.hasNext()) {
          symbol = symbolIterator.next();

          if (symbol instanceof Symbol.Alternative) {
            shouldRead = true;
            fieldSymbol = symbol;
            break;
          }
        }
      }

      if (symbol.kind == Symbol.Kind.TERMINAL) {
        shouldRead = true;
        if (symbolIterator.hasNext()) {
          symbol = symbolIterator.next();

          if (symbol instanceof Symbol.Repeater) {
            fieldSymbol = symbol;
          } else {
            fieldSymbol = symbolIterator.previous();
          }
        } else if (!symbolIterator.hasNext() && getSymbolPrintName(symbol) != null) {
          fieldSymbol = symbol;
        }
        break;
      }
    }

    return FieldAction.fromValues(type, shouldRead, fieldSymbol);
  }

  protected static final class FieldAction {

    private Schema.Type type;
    private boolean shouldRead;
    private Symbol symbol;
    private ListIterator<Symbol> symbolIterator;

    private FieldAction(Schema.Type type, boolean shouldRead, Symbol symbol) {
      this.type = type;
      this.shouldRead = shouldRead;
      this.symbol = symbol;
    }

    private FieldAction(Schema.Type type, boolean shouldRead, ListIterator<Symbol> symbolIterator) {
      this.type = type;
      this.shouldRead = shouldRead;
      this.symbolIterator = symbolIterator;
    }

    public static FieldAction fromValues(Schema.Type type, boolean read, Symbol symbol) {
      return new FieldAction(type, read, symbol);
    }

    public static FieldAction fromValues(Schema.Type type, boolean read, ListIterator<Symbol> symbolIterator) {
      return new FieldAction(type, read, symbolIterator);
    }

    public Schema.Type getType() {
      return type;
    }

    public boolean getShouldRead() {
      return shouldRead;
    }

    public Symbol getSymbol() {
      return symbol;
    }

    public ListIterator<Symbol> getSymbolIterator() {
      return symbolIterator;
    }
  }
}
