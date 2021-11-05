/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111.codec;

import com.linkedin.avroutil1.compatibility.avro111.parsing.ResolvingGrammarGenerator;
import com.linkedin.avroutil1.compatibility.avro111.parsing.Symbol;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A version of ResolvingDecoder that caches the ResolvingGrammarGenerator given a pair of writer and reader schemas,
 * as opposed to the parent class that re-generates the ResolvingGrammarGenerator on each call to DatumReader.read()
 */
public class CachedResolvingDecoder extends ResolvingDecoder {
  private static final ConcurrentHashMap<Schema,ConcurrentHashMap<Schema,Symbol>>
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

  /*skips the symbol type String and returns the size of the String to copy*/
  @Override
  public int readStringSize() throws IOException {
    parser.advance(Symbol.STRING);
    return in.readInt();
  }

  /*skips the symbol type Bytes and returns the size of the Bytes to copy*/
  @Override
  public int readBytesSize() throws IOException {
    parser.advance(Symbol.BYTES);
    return in.readInt();
  }

  /*returns the string data*/
  @Override
  public void readStringData(byte[] bytes, int start, int len) throws IOException {
    in.readFixed(bytes, start, len);
  }

  /*returns the bytes data*/
  @Override
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
  public static Object resolve(Schema writer, Schema reader)
      throws IOException {
    ConcurrentHashMap<Schema,Symbol> cache = SYMBOL_CACHE.computeIfAbsent(writer, k -> new ConcurrentHashMap<>());
    Symbol resolver = cache.get(reader);
    if (resolver == null) {
      resolver =  new ResolvingGrammarGenerator().generate(writer, reader, true);
      cache.put(reader, resolver);
    }
    return resolver;
  }
}