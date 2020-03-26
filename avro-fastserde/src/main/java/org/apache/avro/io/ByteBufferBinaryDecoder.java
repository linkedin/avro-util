package org.apache.avro.io;

import java.io.IOException;


public class ByteBufferBinaryDecoder extends BinaryDecoder {
  int pos;
  public ByteBufferBinaryDecoder(BinaryDecoder decoder) {
    super(decoder.getBuf(),0, decoder.getLimit());
    super.setBuf(decoder.getBuf(), decoder.getPos(), getLimit());
  }
  public ByteBufferBinaryDecoder(byte[] bytes) {
    super(bytes, 0, bytes.length);
  }

  public void readBytes(byte[] bytes,  int length) throws IOException {
    super.doReadBytes(bytes, pos, length);
  }

  public void readBytes(byte[] bytes, int offset, int length) throws IOException {
    super.doReadBytes(bytes, offset, length);
  }
}