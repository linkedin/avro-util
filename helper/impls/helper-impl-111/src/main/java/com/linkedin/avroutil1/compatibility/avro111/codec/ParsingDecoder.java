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
package com.linkedin.avroutil1.compatibility.avro111.codec;

import com.linkedin.avroutil1.compatibility.SkipDecoder;
import com.linkedin.avroutil1.compatibility.avro111.parsing.Parser;
import com.linkedin.avroutil1.compatibility.avro111.parsing.SkipParser;
import com.linkedin.avroutil1.compatibility.avro111.parsing.Symbol;
import org.apache.avro.io.Decoder;

import java.io.IOException;

/**
 * Base class for <a href="parsing/package-summary.html">parser</a>-based
 * {@link Decoder}s.
 */
public abstract class ParsingDecoder extends SkipDecoder implements Parser.ActionHandler, SkipParser.SkipHandler {
  protected final SkipParser parser;

  protected ParsingDecoder(Symbol root) throws IOException {
    this.parser = new SkipParser(root, this, this);
  }

  protected abstract void skipFixed() throws IOException;

  @Override
  public void skipAction() throws IOException {
    parser.popSymbol();
  }

  @Override
  public void skipTopSymbol() throws IOException {
    Symbol top = parser.topSymbol();
    if (top == Symbol.NULL) {
      readNull();
    } else if (top == Symbol.BOOLEAN) {
      readBoolean();
    } else if (top == Symbol.INT) {
      readInt();
    } else if (top == Symbol.LONG) {
      readLong();
    } else if (top == Symbol.FLOAT) {
      readFloat();
    } else if (top == Symbol.DOUBLE) {
      readDouble();
    } else if (top == Symbol.STRING) {
      skipString();
    } else if (top == Symbol.BYTES) {
      skipBytes();
    } else if (top == Symbol.ENUM) {
      readEnum();
    } else if (top == Symbol.FIXED) {
      skipFixed();
    } else if (top == Symbol.UNION) {
      readIndex();
    } else if (top == Symbol.ARRAY_START) {
      skipArray();
    } else if (top == Symbol.MAP_START) {
      skipMap();
    }
  }

}
