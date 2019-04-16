package com.linkedin.avro.fastserde;

import com.google.common.collect.Lists;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;
import org.apache.log4j.Logger;

import static com.linkedin.avro.fastserde.Utils.*;


public abstract class FastDeserializerGeneratorBase<T> {
  public static final String GENERATED_PACKAGE_NAME = "com.linkedin.avro.fastserde.deserialization.generated";
  public static final String GENERATED_SOURCES_PATH = generateSourcePathFromPackageName(GENERATED_PACKAGE_NAME);

  private static final AtomicInteger UNIQUE_ID_BASE = new AtomicInteger(0);

  protected static final Symbol EMPTY_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {
  };
  protected static final Symbol END_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[]{}) {
  };
  private static final Logger LOGGER = Logger.getLogger(FastDeserializerGeneratorBase.class);
  protected final Schema writer;
  protected final Schema reader;
  protected JCodeModel codeModel;
  protected JDefinedClass deserializerClass;
  private File destination;
  private ClassLoader classLoader;
  private String compileClassPath;

  FastDeserializerGeneratorBase(Schema writer, Schema reader, File destination, ClassLoader classLoader,
      String compileClassPath) {
    this.writer = writer;
    this.reader = reader;
    this.destination = destination;
    this.classLoader = classLoader;
    this.compileClassPath = compileClassPath;
    this.codeModel = new JCodeModel();
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
    Schema.Type readerSchemaType = readerSchema.getType();
    if (Schema.Type.RECORD.equals(readerSchemaType)) {
      return readerSchema.getName() + description + "Deserializer" + writerSchemaId + "_" + readerSchemaId;
    } else if (Schema.Type.ARRAY.equals(readerSchemaType)) {
      return "Array" + description + "Deserializer" + writerSchemaId + "_" + readerSchemaId;
    } else if (Schema.Type.MAP.equals(readerSchemaType)) {
      return "Map" + description + "Deserializer" + writerSchemaId + "_" + readerSchemaId;
    }
    throw new FastDeserializerGeneratorException("Unsupported return type: " + readerSchema.getType());
  }

  protected static String getVariableName(String name) {
    return name + UNIQUE_ID_BASE.getAndIncrement();
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

  @SuppressWarnings("unchecked")
  protected Class<FastDeserializer<T>> compileClass(final String className) throws IOException, ClassNotFoundException {
    codeModel.build(destination);

    String filePath = destination.getAbsolutePath() + GENERATED_SOURCES_PATH + className + ".java";
    LOGGER.info("Generated deserializer source file: " + filePath);
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    int compileResult;
    if (compileClassPath != null) {
      compileResult = compiler.run(null, null, null, "-cp", compileClassPath, filePath);
    } else {
      compileResult = compiler.run(null, null, null, filePath);
    }

    if (compileResult != 0) {
      throw new FastDeserializerGeneratorException("unable to compile:" + className);
    }

    return (Class<FastDeserializer<T>>) classLoader.loadClass(GENERATED_PACKAGE_NAME + "." + className);
  }

  protected ListIterator<Symbol> actionIterator(FieldAction action) {
    ListIterator<Symbol> actionIterator = null;

    if (action.getSymbolIterator() != null) {
      actionIterator = action.getSymbolIterator();
    } else if (action.getSymbol().production != null) {
      actionIterator = Lists.newArrayList(reverseSymbolArray(action.getSymbol().production)).listIterator();
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
