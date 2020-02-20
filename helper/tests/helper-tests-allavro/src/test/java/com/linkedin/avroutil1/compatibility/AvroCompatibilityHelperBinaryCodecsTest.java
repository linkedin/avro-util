/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * tests the binary encoder/decoder methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperBinaryCodecsTest {

  @Test
  public void testBinaryCodecs() throws Exception {
    AtomicReference<BinaryEncoder> bufferedEncoderRef = new AtomicReference<>(null);
    AtomicReference<BinaryEncoder> directEncoderRef = new AtomicReference<>(null);
    AtomicReference<BinaryDecoder> bufferedDecoderRef = new AtomicReference<>(null);
    AtomicReference<BinaryDecoder> directDecoderRef = new AtomicReference<>(null);

    for (boolean reuseEncoder : Arrays.asList(false, true)) { //false 1st
      for (boolean reuseDecoder : Arrays.asList(false, true)) { //false 1st
        for (boolean useBufferedEncoder : Arrays.asList(true, false)) {
          for (boolean useBufferedDecoder : Arrays.asList(true, false)) {

            runBinaryEncodeDecodeCycle(
                bufferedEncoderRef, directEncoderRef, bufferedDecoderRef, directDecoderRef,
                reuseEncoder, reuseDecoder, useBufferedEncoder, useBufferedDecoder
            );

          }
        }
      }
    }
  }

  /**
   * convenience test method for reproducing bugs
   */
  @Test
  public void testSimplestCase() throws Exception {
    runBinaryEncodeDecodeCycle(null, null, null, null, false, false, false, false);
  }

  private void runBinaryEncodeDecodeCycle(
      AtomicReference<BinaryEncoder> bufferedEncoderRef,
      AtomicReference<BinaryEncoder> directEncoderRef,
      AtomicReference<BinaryDecoder> bufferedDecoderRef,
      AtomicReference<BinaryDecoder> directDecoderRef,
      boolean reuseEnc,
      boolean reuseDec,
      boolean bufEnc,
      boolean bufDec
  ) throws Exception {
    runEncodeDecodeCycle(new Function<OutputStream, Encoder>() {
      @Override
      public Encoder apply(OutputStream outputStream) {
        BinaryEncoder reuse = null;
        if (reuseEnc) {
          reuse = bufEnc ? bufferedEncoderRef.get() : directEncoderRef.get();
          Assert.assertNotNull(reuse);
        }
        BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(outputStream, bufEnc, reuse);
        Assert.assertNotNull(encoder);
        if (!reuseEnc) {
          if (bufEnc) {
            if (bufferedEncoderRef != null) {
              bufferedEncoderRef.set(encoder);
            }
          } else {
            if (directEncoderRef != null) {
              directEncoderRef.set(encoder);
            }
          }
        }
        return encoder;
      }
    }, new Function<byte[], Decoder>() {
      @Override
      public Decoder apply(byte[] bytes) {
        BinaryDecoder reuse = null;
        if (reuseDec) {
          reuse = bufDec ? bufferedDecoderRef.get() : directDecoderRef.get();
          Assert.assertNotNull(reuse);
        }
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(is, bufDec, reuse);
        Assert.assertNotNull(decoder);
        if (!reuseDec) {
          if (bufDec) {
            if (bufferedDecoderRef != null) {
              bufferedDecoderRef.set(decoder);
            }
          } else {
            if (directDecoderRef != null) {
              directDecoderRef.set(decoder);
            }
          }
        }
        return decoder;
      }
    });
  }

  private void runEncodeDecodeCycle(
      Function<OutputStream, Encoder> encoderFactory,
      Function<byte[], Decoder> decoderFactory
  ) throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder = encoderFactory.apply(os);

    encoder.writeNull();
    encoder.writeBoolean(true);
    encoder.writeBoolean(false);
    encoder.writeInt(Integer.MIN_VALUE);
    encoder.writeInt(-1);
    encoder.writeInt(0);
    encoder.writeInt(1);
    encoder.writeInt(Integer.MAX_VALUE);
    encoder.writeLong(Long.MIN_VALUE);
    encoder.writeLong(((long)Integer.MIN_VALUE) - 1L);
    encoder.writeLong(-1);
    encoder.writeLong(0);
    encoder.writeLong(1);
    encoder.writeLong(((long)Integer.MAX_VALUE) + 1L);
    encoder.writeLong(Long.MAX_VALUE);
    encoder.writeFloat(-Float.MAX_VALUE);
    encoder.writeFloat(-Float.MIN_VALUE);
    encoder.writeFloat(0);
    encoder.writeFloat(Float.MIN_VALUE);
    encoder.writeFloat(Float.MAX_VALUE);
    encoder.writeDouble(-Double.MAX_VALUE);
    encoder.writeDouble(-Double.MIN_VALUE);
    encoder.writeDouble(0);
    encoder.writeDouble(Double.MIN_VALUE);
    encoder.writeDouble(Double.MAX_VALUE);
    encoder.writeString(new Utf8());
    encoder.writeString(new Utf8(""));
    encoder.writeString(new Utf8("whatever"));
    encoder.writeString("");
    encoder.writeString("woha");
    encoder.writeBytes(ByteBuffer.wrap(new byte[] {}));
    encoder.writeBytes(ByteBuffer.wrap(new byte[] {1, 2, 3}));
    encoder.writeBytes(new byte[] {});
    encoder.writeBytes(new byte[] {6, 6, 6});
    encoder.writeBytes(new byte[] {1, 2, 3, 6, 7, 7, 7, 8}, 4, 3);
    encoder.writeFixed(new byte[] {9, 9, 9});
    encoder.writeEnum(42);
    encoder.writeArrayStart();
    encoder.setItemCount(0);
    encoder.writeArrayEnd();
    encoder.writeArrayStart();
    encoder.setItemCount(1);
    encoder.startItem();
    encoder.writeString("item1");
    encoder.writeArrayEnd();
    encoder.writeMapStart();
    encoder.setItemCount(1);
    encoder.startItem();
    encoder.writeString("key");
    encoder.writeString("value");
    encoder.writeMapEnd();
    encoder.writeIndex(7);
    encoder.writeString("8th union branch");

    encoder.flush();
    byte[] data = os.toByteArray();

    Decoder decoder = decoderFactory.apply(data);
    decoder.readNull();
    Assert.assertTrue(decoder.readBoolean());
    Assert.assertFalse(decoder.readBoolean());
    Assert.assertEquals(decoder.readInt(), Integer.MIN_VALUE);
    Assert.assertEquals(decoder.readInt(), -1);
    Assert.assertEquals(decoder.readInt(), 0);
    Assert.assertEquals(decoder.readInt(), 1);
    Assert.assertEquals(decoder.readInt(), Integer.MAX_VALUE);
    Assert.assertEquals(decoder.readLong(), Long.MIN_VALUE);
    Assert.assertEquals(decoder.readLong(), ((long)Integer.MIN_VALUE) - 1L);
    Assert.assertEquals(decoder.readLong(),-1);
    Assert.assertEquals(decoder.readLong(),0);
    Assert.assertEquals(decoder.readLong(),1);
    Assert.assertEquals(decoder.readLong(), ((long)Integer.MAX_VALUE) + 1L);
    Assert.assertEquals(decoder.readLong(), Long.MAX_VALUE);
    Assert.assertEquals(decoder.readFloat(), -Float.MAX_VALUE, 0.0);
    Assert.assertEquals(decoder.readFloat(), -Float.MIN_VALUE, 0.0);
    Assert.assertEquals(decoder.readFloat(), 0f, 0.0);
    Assert.assertEquals(decoder.readFloat(), Float.MIN_VALUE, 0.0);
    Assert.assertEquals(decoder.readFloat(), Float.MAX_VALUE, 0.0);
    Assert.assertEquals(decoder.readDouble(), -Double.MAX_VALUE, 0.0);
    Assert.assertEquals(decoder.readDouble(), -Double.MIN_VALUE, 0.0);
    Assert.assertEquals(decoder.readDouble(), 0d, 0.0);
    Assert.assertEquals(decoder.readDouble(), Double.MIN_VALUE, 0.0);
    Assert.assertEquals(decoder.readDouble(), Double.MAX_VALUE, 0.0);
    Assert.assertEquals(decoder.readString(null), new Utf8());
    Assert.assertEquals(decoder.readString(null), new Utf8(""));
    Assert.assertEquals(decoder.readString(null), new Utf8("whatever"));
    Assert.assertEquals(decoder.readString(null).toString(), "");
    Assert.assertEquals(decoder.readString(null).toString(), "woha");
    Assert.assertEquals(decoder.readBytes(null), ByteBuffer.wrap(new byte[] {}));
    Assert.assertEquals(decoder.readBytes(null), ByteBuffer.wrap(new byte[] {1, 2, 3}));
    Assert.assertEquals(decoder.readBytes(null), ByteBuffer.wrap(new byte[] {}));
    Assert.assertEquals(decoder.readBytes(null), ByteBuffer.wrap(new byte[] {6, 6, 6}));
    Assert.assertEquals(decoder.readBytes(null), ByteBuffer.wrap(new byte[] {7, 7, 7}));
    byte[] output = new byte[3];
    decoder.readFixed(output);
    Assert.assertEquals(output, new byte[] {9, 9, 9});
    Assert.assertEquals(decoder.readEnum(), 42);
    Assert.assertEquals(decoder.readArrayStart(), 0);
    Assert.assertEquals(decoder.readArrayStart(), 1);
    Assert.assertEquals(decoder.readString(null).toString(), "item1");
    Assert.assertEquals(decoder.arrayNext(), 0);
    Assert.assertEquals(decoder.readMapStart(), 1);
    Assert.assertEquals(decoder.readString(null).toString(), "key");
    Assert.assertEquals(decoder.readString(null).toString(), "value");
    Assert.assertEquals(decoder.mapNext(), 0);
    Assert.assertEquals(decoder.readIndex(), 7);
    Assert.assertEquals(decoder.readString(null).toString(), "8th union branch");
  }
}
