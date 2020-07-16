/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package org.apache.avro.io;

/**
 * this class exists to allow us access to package-private classes and methods on class {@link BinaryDecoder}
 *
 * the difference between this method and {@link DecoderFactory#binaryDecoder(byte[], int, int, BinaryDecoder)}
 * is that this method supports configuring custom BinaryDecoder since it does not check class type of BinaryDecoder.
 */
public class Avro15BinaryDecoderAccessUtil {
  public static BinaryDecoder newBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    if (null == reuse) {
      return new BinaryDecoder(bytes, offset, length);
    } else {
      return reuse.configure(bytes, offset, length);
    }
  }
}
