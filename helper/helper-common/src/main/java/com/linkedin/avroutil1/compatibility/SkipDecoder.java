/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.IOException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;


/**
 * An abstract class that extends from Decoder that supports skipping various
 * symbol types and get the variable length of the fields.
 */
public abstract class SkipDecoder extends Decoder {

  /*Skips the Symbol.String and returns the size of the string to copy*/
  public abstract int readStringSize() throws IOException;

  /*Skips the Symbol.BYTES and returns the size of the bytes to copy*/
  public abstract int readBytesSize() throws IOException;

  /*Reads fixed sized String object*/
  public abstract void readStringData(byte[] bytes, int start, int len) throws IOException;

  /*Reads fixed sized Byte object*/
  public abstract void readBytesData(byte[] bytes, int start, int len) throws IOException;

  /**
   * Consume any more data that has been written by the writer but not
   * needed by the reader so that the the underlying decoder is in proper
   * shape for the next record. This situation happens when, for example,
   * the writer writes a record with two fields and the reader needs only the
   * first field.
   *
   * This function should be called after completely decoding an object but
   * before next object can be decoded from the same underlying decoder
   * either directly or through another resolving decoder. If the same resolving
   * decoder is used for the next object as well, calling this method is
   * optional; the state of this resolving decoder ensures that any leftover
   * portions are consumed before the next object is decoded.
   * @throws IOException
   */
  public abstract void drain() throws IOException;

  /*Check if the decoder is a binaryDecoder*/
  public abstract boolean isBinaryDecoder();

  /** Returns the actual order in which the reader's fields will be
   * returned to the reader.
   * Throws a runtime exception if we're not just about to read the
   * field of a record.  Also, this method will consume the field
   * information, and thus may only be called once before reading
   * the field value.  (However, if the client knows the order of
   * incoming fields, then the client does not need to call this
   * method but rather can just start reading the field values.)
   *
   * @throws AvroTypeException If we're not starting a new record
   *
   */
  public abstract Schema.Field[] readFieldOrder() throws IOException;
}
