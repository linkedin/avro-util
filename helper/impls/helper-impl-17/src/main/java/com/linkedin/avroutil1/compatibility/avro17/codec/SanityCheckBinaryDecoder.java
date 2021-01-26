/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17.codec;

import java.io.IOException;
import java.io.InputStream;
import org.apache.avro.util.Utf8;


/**
 * Fixes a bug in the BinaryDecoder that can cause OutOfMemoryError when deserializing corrupt data or deserializing with the incorrect
 * schema. Because this class uses the superclass constructor taking byte[], which is declared with package visiblilty,
 * it has to be in the package org.apache.avro.io.
 *
 * The array size bug is triggered if you try to deserialize corrupt data or try to deserialize data with the wrong schema. When
 * this happens it can cause Avro to incorrectly read the array length and try to allocate a huge (basically a random 32 bit length)
 * array, causing major GC issues. This class protects against this behavior by performing a sanity check during array reads, making
 * sure we are not trying to create an array larger than our total input.
 *
 * Because the byte[] based initialization methods are package private, this class must be in the package "org.apache.avro.io". We could
 * use a properly configured DecoderFactory to create the BinaryDecoder instead of relying on calling init ourselves, but this is not
 * possible since we have to extend BinaryDecoder. Alternatively we could copy the entire source of BinaryDecoder and create our
 * own decoder which does not extend BinaryDecoder. This latter alternative would be more difficult to maintain, and may require pulling
 * out additional package visibility helper classes and methods. We cannot use the public InputStream based constructor, as that
 * constructor creates large read-ahead buffers which is wasteful when reading from a byte[].
 *
 */
public class SanityCheckBinaryDecoder extends BinaryDecoder {
  /**
   * Length of the source data currently being decoded. Used to perform a sanity check on array deserialization to ensure
   * we never try to deserialize an array bigger than our input (e.g. by trying to read corrupt data or reading with the wrong
   * schema).
   */
  private int _srcLen;

  /**
   * Default maximum length to allow if we cannot determine the input length. This happens if we are reinitialized with an
   * InputStream as the data source. Since we can't know the entire length of the stream we use this as an upper bound. Note
   * that when initialized with byte[] data we use the length of the data as the upper limit instead.
   */
  static final int DEFAULT_MAX_ARRAY_LEN = 1000000;

  public SanityCheckBinaryDecoder(byte[] data) {
    // It is important that we reuse a decoder stored in a ThreadLocal. This is because GenericDatumReader will cache the reader
    // in it's own ThreadLocal.
    super(data, 0, data.length);
    _srcLen = data.length;
  }

  public SanityCheckBinaryDecoder(InputStream inputStream) throws IOException {
    // Set the default buffer size to a very small value.
    // Since we are only reading from memory, we don't really need a buffer.
    // Having a buffer will only cause more byte copies.
    // But we still need a small buffer for primitive types.
    final int DEFAULT_BUFFER_SIZE = 32;
    int availableBytes = inputStream.available();

    if (availableBytes > 0)
    {
      int bufferSize = Math.min(availableBytes,DEFAULT_BUFFER_SIZE);
      this.init(bufferSize, inputStream);
      _srcLen = availableBytes;
    }
    else
    {
      this.init(DEFAULT_BUFFER_SIZE, inputStream);
      _srcLen = DEFAULT_MAX_ARRAY_LEN;
    }
  }


  public void init(InputStream in) {
    super.configure(in);
    // Can't determine the length of an InputStream, so use the default instead
    _srcLen = DEFAULT_MAX_ARRAY_LEN;
  }

  void init(int bufferSize, InputStream in) {
    super.configure(in, bufferSize);
    // Can't determine the length of an InputStream, so use the default instead
    _srcLen = DEFAULT_MAX_ARRAY_LEN;
  }

  void init(byte[] data, int offset, int length) {
    super.configure(data, offset, length);
    _srcLen = length;
  }

  @Override
  protected long doReadItemCount() throws IOException {
    long result = readLong();

    if (result < 0) {
      readLong(); // Consume byte-count if present
      result = -result;
    }

    // Common sense check. There is a bug that can return a very large number as the item count.
    // The map or array size definitely cannot exceed the size of the buffer itself
    if(result > _srcLen)
      throw new IOException("Num of items in the data buffer cannot exceed the size of the buffer. " +
          "Length of buffer is " + _srcLen + ", computed size of buffer is " + result);

    return result;
  }

  // This method is copied from BinaryDecoder. A fix has been added to prevent OutOfMemoryException in certain cases.
  @Override
  @SuppressWarnings( "deprecation" )
  public Utf8 readString(Utf8 old) throws IOException {
    int length = readInt();
    // START OF EDIT
    // Common sense check. If we are reading with the wrong schema then the "length = readInt()" will read
    // some arbitrary bunch of bytes which could be huge. That causes the call to result.setLength(length) to
    // allocate a huge byte[] and potentially OOM. This check makes sure we're not trying to allocate more bytes
    // than the incoming byte[] we're deserializing.
    if(length > _srcLen)
      throw new IOException("Length of string cannot exceed the size of the buffer. " +
          "Length of buffer is " + _srcLen + ", computed size of buffer is " + length);
    // END OF EDIT
    Utf8 result = (old != null ? old : new Utf8());
    result.setLength(length);
    doReadBytes(result.getBytes(), 0, length);
    return result;
  }
}