/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.avroutil1.compatibility.avro111.parsing;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.internal.Accessor;

/**
 * The class that generates a resolving grammar to resolve between two schemas.
 */
public class ResolvingGrammarGenerator extends ValidatingGrammarGenerator {

  static {
    //this is done to trigger the static block on the vanilla class
    new org.apache.avro.io.parsing.ResolvingGrammarGenerator();
  }

  /**
   * Resolves the writer schema {@code writer} and the reader schema
   * {@code reader} and returns the start symbol for the grammar generated.
   *
   * @param writer The schema used by the writer
   * @param reader The schema used by the reader
   * @return The start symbol for the resolving grammar
   * @throws IOException
   */
  public final Symbol generate(Schema writer, Schema reader, boolean useFqcns) throws IOException {
    Resolver.Action r = Resolver.resolve(writer, reader);
    return Symbol.root(generate(r, new HashMap<>(), useFqcns));
  }

  /**
   * Takes a {@link Resolver.Action} for resolving two schemas and returns the
   * start symbol for a grammar that implements that resolution. If the action is
   * for a record and there's already a symbol for that record in {@code seen},
   * then that symbol is returned. Otherwise a new symbol is generated and
   * returned.
   *
   * @param action The resolver to be implemented
   * @param seen   The &lt;Action&gt; to symbol map of start symbols of resolving
   *               grammars so far.
   * @return The start symbol for the resolving grammar
   * @throws IOException
   */
  private Symbol generate(Resolver.Action action, Map<Object, Symbol> seen, boolean useFqcns) throws IOException {
    if (action instanceof Resolver.DoNothing) {
      return simpleGen(action.writer, seen, useFqcns);

    } else if (action instanceof Resolver.ErrorAction) {
      return Symbol.error(action.toString());

    } else if (action instanceof Resolver.Skip) {
      return Symbol.skipAction(simpleGen(action.writer, seen, useFqcns));

    } else if (action instanceof Resolver.Promote) {
      return Symbol.resolve(simpleGen(action.writer, seen, useFqcns), simpleGen(action.reader, seen, useFqcns));

    } else if (action instanceof Resolver.ReaderUnion) {
      Resolver.ReaderUnion ru = (Resolver.ReaderUnion) action;
      Symbol s = generate(ru.actualAction, seen, useFqcns);
      return Symbol.seq(Symbol.unionAdjustAction(ru.firstMatch, s), Symbol.UNION);

    } else if (action.writer.getType() == Schema.Type.ARRAY) {
      Symbol es = generate(((Resolver.Container) action).elementAction, seen, useFqcns);
      return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END, es), Symbol.ARRAY_START);

    } else if (action.writer.getType() == Schema.Type.MAP) {
      Symbol es = generate(((Resolver.Container) action).elementAction, seen, useFqcns);
      return Symbol.seq(Symbol.repeat(Symbol.MAP_END, es, Symbol.STRING), Symbol.MAP_START);

    } else if (action.writer.getType() == Schema.Type.UNION) {
      if (((Resolver.WriterUnion) action).unionEquiv) {
        return simpleGen(action.writer, seen, useFqcns);
      }
      Resolver.Action[] branches = ((Resolver.WriterUnion) action).actions;
      Symbol[] symbols = new Symbol[branches.length];
      String[] oldLabels = new String[branches.length];
      String[] newLabels = new String[branches.length];
      int i = 0;
      for (Resolver.Action branch : branches) {
        symbols[i] = generate(branch, seen, useFqcns);
        Schema schema = action.writer.getTypes().get(i);
        oldLabels[i] = schema.getName();
        newLabels[i] = schema.getFullName();
        i++;
      }
      return Symbol.seq(Symbol.alt(symbols, oldLabels, newLabels, useFqcns), Symbol.WRITER_UNION_ACTION);
    } else if (action instanceof Resolver.EnumAdjust) {
      Resolver.EnumAdjust e = (Resolver.EnumAdjust) action;
      Object[] adjs = new Object[e.adjustments.length];
      for (int i = 0; i < adjs.length; i++) {
        adjs[i] = (0 <= e.adjustments[i] ? new Integer(e.adjustments[i])
            : "No match for " + e.writer.getEnumSymbols().get(i));
      }
      return Symbol.seq(Symbol.enumAdjustAction(e.reader.getEnumSymbols().size(), adjs), Symbol.ENUM);

    } else if (action instanceof Resolver.RecordAdjust) {
      Symbol result = seen.get(action);
      if (result == null) {
        final Resolver.RecordAdjust ra = (Resolver.RecordAdjust) action;
        int defaultCount = ra.readerOrder.length - ra.firstDefault;
        int count = 1 + ra.fieldActions.length + 3 * defaultCount;
        final Symbol[] production = new Symbol[count];
        result = Symbol.seq(production);
        seen.put(action, result);
        production[--count] = Symbol.fieldOrderAction(ra.readerOrder);

        final Resolver.Action[] actions = ra.fieldActions;
        for (Resolver.Action wfa : actions) {
          production[--count] = generate(wfa, seen, useFqcns);
        }
        for (int i = ra.firstDefault; i < ra.readerOrder.length; i++) {
          final Field rf = ra.readerOrder[i];
          byte[] bb = getBinary(rf.schema(), Accessor.defaultValue(rf));
          production[--count] = Symbol.defaultStartAction(bb);
          production[--count] = simpleGen(rf.schema(), seen, useFqcns);
          production[--count] = Symbol.DEFAULT_END_ACTION;
        }
      }
      return result;
    }

    throw new IllegalArgumentException("Unrecognized Resolver.Action: " + action);
  }

  private Symbol simpleGen(Schema s, Map<Object, Symbol> seen, boolean useFqcns) {
    switch (s.getType()) {
      case NULL:
        return Symbol.NULL;
      case BOOLEAN:
        return Symbol.BOOLEAN;
      case INT:
        return Symbol.INT;
      case LONG:
        return Symbol.LONG;
      case FLOAT:
        return Symbol.FLOAT;
      case DOUBLE:
        return Symbol.DOUBLE;
      case BYTES:
        return Symbol.BYTES;
      case STRING:
        return Symbol.STRING;

      case FIXED:
        return Symbol.seq(Symbol.intCheckAction(s.getFixedSize()), Symbol.FIXED);

      case ENUM:
        return Symbol.seq(Symbol.enumAdjustAction(s.getEnumSymbols().size(), null), Symbol.ENUM);

      case ARRAY:
        return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END, simpleGen(s.getElementType(), seen, useFqcns)), Symbol.ARRAY_START);

      case MAP:
        return Symbol.seq(Symbol.repeat(Symbol.MAP_END, simpleGen(s.getValueType(), seen, useFqcns), Symbol.STRING),
            Symbol.MAP_START);

      case UNION: {
        final List<Schema> subs = s.getTypes();
        final Symbol[] symbols = new Symbol[subs.size()];
        final String[] oldLabels = new String[subs.size()];
        final String[] newLabels = new String[subs.size()];
        int i = 0;
        for (Schema b : s.getTypes()) {
          symbols[i] = simpleGen(b, seen, useFqcns);
          oldLabels[i] = b.getName();
          newLabels[i] = b.getFullName();
          i++;
        }
        return Symbol.seq(Symbol.alt(symbols, oldLabels, newLabels, useFqcns), Symbol.UNION);
      }

      case RECORD: {
        Symbol result = seen.get(s);
        if (result == null) {
          final Symbol[] production = new Symbol[s.getFields().size() + 1];
          result = Symbol.seq(production);
          seen.put(s, result);
          int i = production.length;
          production[--i] = Symbol.fieldOrderAction(s.getFields().toArray(new Field[0]));
          for (Field f : s.getFields()) {
            production[--i] = simpleGen(f.schema(), seen, useFqcns);
          }
          // FieldOrderAction is needed even though the field-order hasn't changed,
          // because the _reader_ doesn't know the field order hasn't changed, and
          // thus it will probably call {@ ResolvingDecoder.fieldOrder} to find out.
        }
        return result;
      }

      default:
        throw new IllegalArgumentException("Unexpected schema: " + s);
    }
  }

  private static EncoderFactory factory = new EncoderFactory().configureBufferSize(32);

  /**
   * Returns the Avro binary encoded version of {@code n} according to the schema
   * {@code s}.
   *
   * @param s The schema for encoding
   * @param n The Json node that has the value to be encoded.
   * @return The binary encoded version of {@code n}.
   * @throws IOException
   */
  protected static byte[] getBinary(Schema s, JsonNode n) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder e = factory.binaryEncoder(out, null);
    encode(e, s, n);
    e.flush();
    return out.toByteArray();
  }

  /**
   * Encodes the given Json node {@code n} on to the encoder {@code e} according
   * to the schema {@code s}.
   *
   * @param e The encoder to encode into.
   * @param s The schema for the object being encoded.
   * @param n The Json node to encode.
   * @throws IOException
   */
  public static void encode(Encoder e, Schema s, JsonNode n) throws IOException {
    switch (s.getType()) {
      case RECORD:
        for (Field f : s.getFields()) {
          String name = f.name();
          JsonNode v = n.get(name);
          if (v == null) {
            v = Accessor.defaultValue(f);
          }
          if (v == null) {
            throw new AvroTypeException("No default value for: " + name);
          }
          encode(e, f.schema(), v);
        }
        break;
      case ENUM:
        e.writeEnum(s.getEnumOrdinal(n.textValue()));
        break;
      case ARRAY:
        e.writeArrayStart();
        e.setItemCount(n.size());
        Schema i = s.getElementType();
        for (JsonNode node : n) {
          e.startItem();
          encode(e, i, node);
        }
        e.writeArrayEnd();
        break;
      case MAP:
        e.writeMapStart();
        e.setItemCount(n.size());
        Schema v = s.getValueType();
        for (Iterator<String> it = n.fieldNames(); it.hasNext();) {
          e.startItem();
          String key = it.next();
          e.writeString(key);
          encode(e, v, n.get(key));
        }
        e.writeMapEnd();
        break;
      case UNION:
        e.writeIndex(0);
        encode(e, s.getTypes().get(0), n);
        break;
      case FIXED:
        if (!n.isTextual())
          throw new AvroTypeException("Non-string default value for fixed: " + n);
        byte[] bb = n.textValue().getBytes(StandardCharsets.ISO_8859_1);
        if (bb.length != s.getFixedSize()) {
          bb = Arrays.copyOf(bb, s.getFixedSize());
        }
        e.writeFixed(bb);
        break;
      case STRING:
        if (!n.isTextual())
          throw new AvroTypeException("Non-string default value for string: " + n);
        e.writeString(n.textValue());
        break;
      case BYTES:
        if (!n.isTextual())
          throw new AvroTypeException("Non-string default value for bytes: " + n);
        e.writeBytes(n.textValue().getBytes(StandardCharsets.ISO_8859_1));
        break;
      case INT:
        if (!n.isNumber())
          throw new AvroTypeException("Non-numeric default value for int: " + n);
        e.writeInt(n.intValue());
        break;
      case LONG:
        if (!n.isNumber())
          throw new AvroTypeException("Non-numeric default value for long: " + n);
        e.writeLong(n.longValue());
        break;
      case FLOAT:
        if (!n.isNumber())
          throw new AvroTypeException("Non-numeric default value for float: " + n);
        e.writeFloat((float) n.doubleValue());
        break;
      case DOUBLE:
        if (!n.isNumber())
          throw new AvroTypeException("Non-numeric default value for double: " + n);
        e.writeDouble(n.doubleValue());
        break;
      case BOOLEAN:
        if (!n.isBoolean())
          throw new AvroTypeException("Non-boolean default for boolean: " + n);
        e.writeBoolean(n.booleanValue());
        break;
      case NULL:
        if (!n.isNull())
          throw new AvroTypeException("Non-null default value for null type: " + n);
        e.writeNull();
        break;
    }
  }

  /**
   * Clever trick which differentiates items put into
   * <code>seen</code> by {@link ValidatingGrammarGenerator validating()}
   * from those put in by {@link ValidatingGrammarGenerator resolving()}.
   */
  static class LitS2 extends LitS {
    public Schema expected;
    public LitS2(Schema actual, Schema expected) {
      super(actual);
      this.expected = expected;
    }
    public boolean equals(Object o) {
      if (! (o instanceof LitS2)) return false;
      LitS2 other = (LitS2) o;
      return actual == other.actual && expected == other.expected;
    }
    public int hashCode() {
      return super.hashCode() + expected.hashCode();
    }
  }
}
