/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


public class Avro18SchemaConstructable implements SpecificData.SchemaConstructable {
  private final Schema schema;

  public Avro18SchemaConstructable(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }
}
