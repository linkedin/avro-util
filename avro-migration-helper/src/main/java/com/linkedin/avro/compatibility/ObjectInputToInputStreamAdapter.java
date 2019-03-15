/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;


/**
 * see ExternalizableInput in avro 1.8
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
