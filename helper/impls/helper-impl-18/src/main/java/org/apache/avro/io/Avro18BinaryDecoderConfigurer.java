/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import org.apache.avro.io.BinaryDecoder;


public class Avro18BinaryDecoderConfigurer {
  public BinaryDecoder configureBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    return reuse.configure(bytes, offset, length);
  }
}
