/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package org.apache.avro.io;

public class Avro15BinaryDecoderConfigurer {
  public BinaryDecoder configureBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    return reuse.configure(bytes, offset, length);
  }
}
