/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

/**
 * An abstract class that extends from Decoder that supports skipping various
 * symbol types and get the variable length of the fields.
 */
public abstract class SkipDecoder extends Decoder {

  public abstract int readStringSize() throws IOException;

  public abstract int readBytesSize() throws IOException;

  public abstract void readStringData(byte[] bytes, int start, int len) throws IOException;

  public abstract void readBytesData(byte[] bytes, int start, int len) throws IOException;

  public abstract void drain() throws IOException;

  public abstract boolean isBinaryDecoder();

  public abstract Schema.Field[] readFieldOrder() throws IOException;
}
