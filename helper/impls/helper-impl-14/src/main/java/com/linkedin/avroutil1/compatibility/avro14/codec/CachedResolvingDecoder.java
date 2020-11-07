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
package com.linkedin.avroutil1.compatibility.avro14.codec;

import com.linkedin.avroutil1.compatibility.avro14.parsing.CachedResolvingGrammarGenerator;
import com.linkedin.avroutil1.compatibility.avro14.parsing.Symbol;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;


public class CachedResolvingDecoder extends ResolvingDecoder {
  private static final ConcurrentHashMap<Schema, ConcurrentHashMap<Schema, Symbol>>
      SYMBOL_CACHE = new ConcurrentHashMap<>();

  public CachedResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException {
    this(resolve(writer, reader), in);
  }

  /**
   * Constructs a CachedResolvingDecoder using the given resolver.
   * The resolver must have been returned by a previous call to
   * {@link #resolve(Schema, Schema)}.
   * @param resolver  The resolver to use.
   * @param in  The underlying decoder.
   * @throws IOException
   */
  public CachedResolvingDecoder(Object resolver, Decoder in)
      throws IOException {
    super(resolver, in);
  }
  /*skips the symbol and returns the size of the string to copy*/
  public int readStringSize() throws IOException {
    parser.advance(Symbol.STRING);
    return in.readInt();
  }
  /*skips the symbol and returns the size of the bytes to copy*/
  public int readBytesSize() throws IOException {
    parser.advance(Symbol.BYTES);
    return in.readInt();
  }
  /*returns the string data*/
  public void readStringData(byte[] bytes, int start, int len) throws IOException {
    in.readFixed(bytes, start, len);
  }
  /*returns the bytes data*/
  public void readBytesData(byte[] bytes, int start, int len) throws IOException {
    in.readFixed(bytes, start, len);
  }

  public boolean isBinaryDecoder() {
    return this.in instanceof BinaryDecoder;
  }
  /**
   * Produces an opaque resolver that can be used to construct a new
   * {@link ResolvingDecoder (Object, Decoder)}. The
   * returned Object is immutable and hence can be simultaneously used
   * in many ResolvingDecoders. This method is reasonably expensive, the
   * users are encouraged to cache the result.
   *
   * @param writer  The writer's schema.
   * @param reader  The reader's schema.
   * @return  The opaque reolver.
   * @throws IOException
   */
  public static Object resolve(Schema writer, Schema reader) throws IOException {
    ConcurrentHashMap<Schema,Symbol> cache = SYMBOL_CACHE.computeIfAbsent(writer, k -> new ConcurrentHashMap<>());
    Symbol resolver = cache.get(reader);
    if (resolver == null) {
      resolver =  new CachedResolvingGrammarGenerator().generate(writer, reader);
      cache.put(reader, resolver);
    }
    return resolver;
  }
}
