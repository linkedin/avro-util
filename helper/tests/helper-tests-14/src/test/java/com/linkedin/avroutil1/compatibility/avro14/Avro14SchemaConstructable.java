package com.linkedin.avroutil1.compatibility.avro14;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificDatumReader;


public class Avro14SchemaConstructable implements SpecificDatumReader.SchemaConstructable {
  private final Schema schema;

  public Avro14SchemaConstructable(Schema schema) {
    this.schema = schema;
  }

  public Schema getSchema() {
    return schema;
  }
}
