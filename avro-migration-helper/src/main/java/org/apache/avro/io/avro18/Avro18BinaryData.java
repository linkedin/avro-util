/*
 * Copyright 2020 LinkedIn Corp.
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

package org.apache.avro.io.avro18;


/**
 * Back-port {@literal BufferedBinaryEncoder} from Avro-1.8, so that Avro-1.4 could use it to improve serialization performance.
 * We also removed a bunch of unrelated methods, so that we don't need to back-port Decoder related code here.
 */
public class Avro18BinaryData {

  private Avro18BinaryData() {}                      // no public ctor

  /** Encode a boolean to the byte array at the given position. Will throw
   * IndexOutOfBounds if the position is not valid.
   * @param b boolean to encode
   * @param buf buffer to encode into
   * @param pos position in buffer to start encoding into
   * @return The number of bytes written to the buffer, 1.
   */
  public static int encodeBoolean(boolean b, byte[] buf, int pos) {
    buf[pos] = b ? (byte) 1 : (byte) 0;
    return 1;
  }

  /** Encode an integer to the byte array at the given position. Will throw
   * IndexOutOfBounds if it overflows. Users should ensure that there are at
   * least 5 bytes left in the buffer before calling this method.
   * @param n int to encode
   * @param buf buffer to encode into
   * @param pos position in buffer to start encoding into
   * @return The number of bytes written to the buffer, between 1 and 5.
   */
  public static int encodeInt(int n, byte[] buf, int pos) {
    // move sign to low-order bit, and flip others if negative
    n = (n << 1) ^ (n >> 31);
    int start = pos;
    if ((n & ~0x7F) != 0) {
      buf[pos++] = (byte)((n | 0x80) & 0xFF);
      n >>>= 7;
      if (n > 0x7F) {
        buf[pos++] = (byte)((n | 0x80) & 0xFF);
        n >>>= 7;
        if (n > 0x7F) {
          buf[pos++] = (byte)((n | 0x80) & 0xFF);
          n >>>= 7;
          if (n > 0x7F) {
            buf[pos++] = (byte)((n | 0x80) & 0xFF);
            n >>>= 7;
          }
        }
      }
    }
    buf[pos++] = (byte) n;
    return pos - start;
  }

  /** Encode a long to the byte array at the given position. Will throw
   * IndexOutOfBounds if it overflows. Users should ensure that there are at
   * least 10 bytes left in the buffer before calling this method.
   * @param n long to encode
   * @param buf buffer to encode into
   * @param pos position in buffer to start encoding into
   * @return The number of bytes written to the buffer, between 1 and 10.
   */
  public static int encodeLong(long n, byte[] buf, int pos) {
    // move sign to low-order bit, and flip others if negative
    n = (n << 1) ^ (n >> 63);
    int start = pos;
    if ((n & ~0x7FL) != 0) {
      buf[pos++] = (byte)((n | 0x80) & 0xFF);
      n >>>= 7;
      if (n > 0x7F) {
        buf[pos++] = (byte)((n | 0x80) & 0xFF);
        n >>>= 7;
        if (n > 0x7F) {
          buf[pos++] = (byte)((n | 0x80) & 0xFF);
          n >>>= 7;
          if (n > 0x7F) {
            buf[pos++] = (byte)((n | 0x80) & 0xFF);
            n >>>= 7;
            if (n > 0x7F) {
              buf[pos++] = (byte)((n | 0x80) & 0xFF);
              n >>>= 7;
              if (n > 0x7F) {
                buf[pos++] = (byte)((n | 0x80) & 0xFF);
                n >>>= 7;
                if (n > 0x7F) {
                  buf[pos++] = (byte)((n | 0x80) & 0xFF);
                  n >>>= 7;
                  if (n > 0x7F) {
                    buf[pos++] = (byte)((n | 0x80) & 0xFF);
                    n >>>= 7;
                    if (n > 0x7F) {
                      buf[pos++] = (byte)((n | 0x80) & 0xFF);
                      n >>>= 7;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    buf[pos++] = (byte) n;
    return pos - start;
  }

  /** Encode a float to the byte array at the given position. Will throw
   * IndexOutOfBounds if it overflows. Users should ensure that there are at
   * least 4 bytes left in the buffer before calling this method.
   * @param f float to encode
   * @param buf buffer to encode into
   * @param pos position in buffer to start encoding into
   * @return Returns the number of bytes written to the buffer, 4.
   */
  public static int encodeFloat(float f, byte[] buf, int pos) {
    int len = 1;
    int bits = Float.floatToRawIntBits(f);
    // hotspot compiler works well with this variant
    buf[pos]         = (byte)((bits       ) & 0xFF);
    buf[pos + len++] = (byte)((bits >>>  8) & 0xFF);
    buf[pos + len++] = (byte)((bits >>> 16) & 0xFF);
    buf[pos + len++] = (byte)((bits >>> 24) & 0xFF);
    return 4;
  }

  /** Encode a double to the byte array at the given position. Will throw
   * IndexOutOfBounds if it overflows. Users should ensure that there are at
   * least 8 bytes left in the buffer before calling this method.
   * @param d double to encode
   * @param buf buffer to encode into
   * @param pos position in buffer to start encoding into
   * @return Returns the number of bytes written to the buffer, 8.
   */
  public static int encodeDouble(double d, byte[] buf, int pos) {
    long bits = Double.doubleToRawLongBits(d);
    int first = (int)(bits & 0xFFFFFFFF);
    int second = (int)((bits >>> 32) & 0xFFFFFFFF);
    // the compiler seems to execute this order the best, likely due to
    // register allocation -- the lifetime of constants is minimized.
    buf[pos]     = (byte)((first        ) & 0xFF);
    buf[pos + 4] = (byte)((second       ) & 0xFF);
    buf[pos + 5] = (byte)((second >>>  8) & 0xFF);
    buf[pos + 1] = (byte)((first >>>   8) & 0xFF);
    buf[pos + 2] = (byte)((first >>>  16) & 0xFF);
    buf[pos + 6] = (byte)((second >>> 16) & 0xFF);
    buf[pos + 7] = (byte)((second >>> 24) & 0xFF);
    buf[pos + 3] = (byte)((first >>>  24) & 0xFF);
    return 8;
  }
}
