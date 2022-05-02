/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import java.util.Arrays;
import org.apache.avro.Schema;


public class SchemaId {

  private final byte[] md5;

  public SchemaId(byte[] md5) {
    super();
    this.md5 = AvroSchemaBuilderUtils.notNull(md5);
  }

  public static SchemaId forSchema(Schema schema) {
    return new SchemaId(AvroSchemaBuilderUtils.schemaToMd5(schema));
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!obj.getClass().equals(SchemaId.class)) {
      return false;
    }
    SchemaId id = (SchemaId) obj;
    return AvroSchemaBuilderUtils.equals(md5, id.md5);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(md5);
  }

  @Override
  public String toString() {
    return AvroSchemaBuilderUtils.hex(md5);
  }
}