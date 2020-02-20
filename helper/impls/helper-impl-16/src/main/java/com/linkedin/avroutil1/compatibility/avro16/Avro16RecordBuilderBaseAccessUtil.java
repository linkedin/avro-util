/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.data.RecordBuilderBase;
import org.apache.avro.generic.IndexedRecord;


public class Avro16RecordBuilderBaseAccessUtil extends RecordBuilderBase<IndexedRecord> {
  private final static Avro16RecordBuilderBaseAccessUtil INSTANCE = new Avro16RecordBuilderBaseAccessUtil();

  private Avro16RecordBuilderBaseAccessUtil() {
    super(Schema.create(Schema.Type.NULL), null);
  }

  @Override
  public IndexedRecord build() {
    throw new UnsupportedOperationException("should never get here");
  }

  static Object getDefaultValue(Schema.Field field) {
    try {
      return INSTANCE.defaultValue(field);
    } catch (IOException e) {
      throw new IllegalStateException("unexpected exception getting default value for " + field, e);
    }
  }
}
