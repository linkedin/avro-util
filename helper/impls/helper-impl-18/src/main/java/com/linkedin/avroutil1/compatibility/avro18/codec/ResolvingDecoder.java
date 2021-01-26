/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.avroutil1.compatibility.avro18.codec;

import com.linkedin.avroutil1.compatibility.avro18.parsing.ResolvingGrammarGenerator;
import com.linkedin.avroutil1.compatibility.avro18.parsing.Symbol;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

public class ResolvingDecoder extends ValidatingDecoder {
  private Decoder backup;
  private static final Charset UTF8 = Charset.forName("UTF-8");

  ResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException {
    this(resolve(writer, reader), in);
  }

  public ResolvingDecoder(Object resolver, Decoder in) throws IOException {
    super((Symbol)resolver, in);
  }

  public static Object resolve(Schema writer, Schema reader) throws IOException {
    if (null == writer) {
      throw new NullPointerException("writer cannot be null!");
    } else if (null == reader) {
      throw new NullPointerException("reader cannot be null!");
    } else {
      return (new ResolvingGrammarGenerator()).generate(writer, reader, true);
    }
  }

  public final Field[] readFieldOrder() throws IOException {
    return ((Symbol.FieldOrderAction)this.parser.advance(Symbol.FIELD_ACTION)).fields;
  }

  @Override
  public final void drain() throws IOException {
    this.parser.processImplicitActions();
  }

  public long readLong() throws IOException {
    Symbol actual = this.parser.advance(Symbol.LONG);
    if (actual == Symbol.INT) {
      return (long)this.in.readInt();
    } else if (actual == Symbol.DOUBLE) {
      return (long)this.in.readDouble();
    } else {
      assert actual == Symbol.LONG;

      return this.in.readLong();
    }
  }

  public float readFloat() throws IOException {
    Symbol actual = this.parser.advance(Symbol.FLOAT);
    if (actual == Symbol.INT) {
      return (float)this.in.readInt();
    } else if (actual == Symbol.LONG) {
      return (float)this.in.readLong();
    } else {
      assert actual == Symbol.FLOAT;

      return this.in.readFloat();
    }
  }

  public double readDouble() throws IOException {
    Symbol actual = this.parser.advance(Symbol.DOUBLE);
    if (actual == Symbol.INT) {
      return (double)this.in.readInt();
    } else if (actual == Symbol.LONG) {
      return (double)this.in.readLong();
    } else if (actual == Symbol.FLOAT) {
      return (double)this.in.readFloat();
    } else {
      assert actual == Symbol.DOUBLE;

      return this.in.readDouble();
    }
  }

  public Utf8 readString(Utf8 old) throws IOException {
    Symbol actual = this.parser.advance(Symbol.STRING);
    if (actual == Symbol.BYTES) {
      return new Utf8(this.in.readBytes((ByteBuffer)null).array());
    } else {
      assert actual == Symbol.STRING;

      return this.in.readString(old);
    }
  }

  public String readString() throws IOException {
    Symbol actual = this.parser.advance(Symbol.STRING);
    if (actual == Symbol.BYTES) {
      return new String(this.in.readBytes((ByteBuffer)null).array(), UTF8);
    } else {
      assert actual == Symbol.STRING;

      return this.in.readString();
    }
  }

  public void skipString() throws IOException {
    Symbol actual = this.parser.advance(Symbol.STRING);
    if (actual == Symbol.BYTES) {
      this.in.skipBytes();
    } else {
      assert actual == Symbol.STRING;

      this.in.skipString();
    }

  }

  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    Symbol actual = this.parser.advance(Symbol.BYTES);
    if (actual == Symbol.STRING) {
      Utf8 s = this.in.readString((Utf8)null);
      return ByteBuffer.wrap(s.getBytes(), 0, s.getByteLength());
    } else {
      assert actual == Symbol.BYTES;

      return this.in.readBytes(old);
    }
  }

  public void skipBytes() throws IOException {
    Symbol actual = this.parser.advance(Symbol.BYTES);
    if (actual == Symbol.STRING) {
      this.in.skipString();
    } else {
      assert actual == Symbol.BYTES;

      this.in.skipBytes();
    }

  }

  public int readEnum() throws IOException {
    this.parser.advance(Symbol.ENUM);
    Symbol.EnumAdjustAction top = (Symbol.EnumAdjustAction)this.parser.popSymbol();
    int n = this.in.readEnum();
    Object o = top.adjustments[n];
    if (o instanceof Integer) {
      return (Integer)o;
    } else {
      throw new AvroTypeException((String)o);
    }
  }

  public int readIndex() throws IOException {
    this.parser.advance(Symbol.UNION);
    Symbol.UnionAdjustAction top = (Symbol.UnionAdjustAction)this.parser.popSymbol();
    this.parser.pushSymbol(top.symToParse);
    return top.rindex;
  }

  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldOrderAction) {
      return input == Symbol.FIELD_ACTION ? top : null;
    } else if (top instanceof Symbol.ResolvingAction) {
      Symbol.ResolvingAction t = (Symbol.ResolvingAction)top;
      if (t.reader != input) {
        throw new AvroTypeException("Found " + t.reader + " while looking for " + input);
      } else {
        return t.writer;
      }
    } else {
      if (top instanceof Symbol.SkipAction) {
        Symbol symToSkip = ((Symbol.SkipAction)top).symToSkip;
        this.parser.skipSymbol(symToSkip);
      } else if (top instanceof Symbol.WriterUnionAction) {
        Symbol.Alternative branches = (Symbol.Alternative)this.parser.popSymbol();
        this.parser.pushSymbol(branches.getSymbol(this.in.readIndex()));
      } else {
        if (top instanceof Symbol.ErrorAction) {
          throw new AvroTypeException(((Symbol.ErrorAction)top).msg);
        }

        if (top instanceof Symbol.DefaultStartAction) {
          Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction)top;
          this.backup = this.in;
          this.in = DecoderFactory.get().binaryDecoder(dsa.contents, (BinaryDecoder)null);
        } else {
          if (top != Symbol.DEFAULT_END_ACTION) {
            throw new AvroTypeException("Unknown action: " + top);
          }

          this.in = this.backup;
        }
      }

      return null;
    }
  }

  public void skipAction() throws IOException {
    Symbol top = this.parser.popSymbol();
    if (top instanceof Symbol.ResolvingAction) {
      this.parser.pushSymbol(((Symbol.ResolvingAction)top).writer);
    } else if (top instanceof Symbol.SkipAction) {
      this.parser.pushSymbol(((Symbol.SkipAction)top).symToSkip);
    } else if (top instanceof Symbol.WriterUnionAction) {
      Symbol.Alternative branches = (Symbol.Alternative)this.parser.popSymbol();
      this.parser.pushSymbol(branches.getSymbol(this.in.readIndex()));
    } else {
      if (top instanceof Symbol.ErrorAction) {
        throw new AvroTypeException(((Symbol.ErrorAction)top).msg);
      }
      if (top instanceof Symbol.DefaultStartAction) {
        Symbol.DefaultStartAction dsa = (Symbol.DefaultStartAction)top;
        this.backup = this.in;
        this.in = DecoderFactory.get().binaryDecoder(dsa.contents, (BinaryDecoder)null);
      } else if (top == Symbol.DEFAULT_END_ACTION) {
        this.in = this.backup;
      }
    }
  }
}
