package com.linkedin.avroutil1.compatibility.avro19;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


public class Avro19SchemaConstructable implements SpecificData.SchemaConstructable {
  private final Schema schema;

  public Avro19SchemaConstructable(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }
}
