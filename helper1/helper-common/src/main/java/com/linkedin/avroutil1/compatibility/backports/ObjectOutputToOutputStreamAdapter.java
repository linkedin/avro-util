package com.linkedin.avroutil1.compatibility.backports;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * This is a backport of org.apache.avro.specific.ExternalizableOutput in avro 1.8+
 * it permits {@link java.io.Externalizable} implementations that write to an OutputStream.
 */
public class ObjectOutputToOutputStreamAdapter extends OutputStream {
  private final ObjectOutput out;

  public ObjectOutputToOutputStreamAdapter(ObjectOutput out) {
    this.out = out;
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public void write(int c) throws IOException {
    out.write(c);
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b, int offset, int len) throws IOException {
    out.write(b, offset, len);
  }
}
