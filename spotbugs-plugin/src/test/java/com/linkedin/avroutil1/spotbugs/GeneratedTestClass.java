/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import org.apache.avro.Schema;


public class GeneratedTestClass extends org.apache.avro.specific.SpecificRecordBase
    implements org.apache.avro.specific.SpecificRecord {
  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public Object get(int field) {
    return null;
  }

  @Override
  public void put(int field, Object value) {

  }

  public void serializeSchemaUsingToString() {
    Schema schema = Schema.create(Schema.Type.STRING);
    String serialized = schema.toString();
    String.valueOf(schema);
  }
}
