/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;


public class Avro15SchemaConstructable implements SpecificDatumReader.SchemaConstructable {
  private final Schema schema;

  public Avro15SchemaConstructable(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }
}
