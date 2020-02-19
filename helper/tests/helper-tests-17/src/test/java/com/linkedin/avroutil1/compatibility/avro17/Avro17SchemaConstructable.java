package com.linkedin.avroutil1.compatibility.avro17;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


public class Avro17SchemaConstructable implements SpecificData.SchemaConstructable {
  private final Schema schema;

  public Avro17SchemaConstructable(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }
}
