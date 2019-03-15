/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;


/**
 * see ExternalizableOutput in avro 1.8
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