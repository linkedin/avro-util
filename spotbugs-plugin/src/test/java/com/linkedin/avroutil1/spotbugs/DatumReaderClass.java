/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;

public abstract class DatumReaderClass<D> extends GenericDatumReader<D> {
  @Override
  protected String readString(Object old, Schema s,
      Decoder in) throws IOException {
    return super.readString(old, s, in).toString();
  }
}
