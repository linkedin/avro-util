package com.linkedin.avroutil1.compatibility.backports;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;


/**
 * This is a backport of org.apache.avro.specific.ExternalizableInput in avro 1.8+
 * it permits {@link java.io.Externalizable} implementations that deserialize from an InputStream
 */
public class ObjectInputToInputStreamAdapter extends InputStream {
  private final ObjectInput in;

  public ObjectInputToInputStreamAdapter(ObjectInput in) {
    this.in = in;
  }

  @Override public int available() throws IOException {
    return in.available();
  }

  @Override public void close() throws IOException {
    in.close();
  }

  @Override public boolean  markSupported() {
    return false;
  }

  @Override public int read() throws IOException {
    return in.read();
  }

  @Override public int read(byte[] b) throws IOException {
    return in.read(b);
  }

  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    return in.read(b, offset, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return in.skip(n);
  }
}
