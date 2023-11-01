package com.linkedin.avro.api.impl;

import org.apache.avro.Schema;


public class PrimitiveField extends Schema.Field {
  private final int primitivePosition;

  public PrimitiveField(String name, Schema schema, String doc, Object defaultValue, Order order, int primitivePosition) {
    super(name, schema, doc, defaultValue, order);
    this.primitivePosition = primitivePosition;
  }

  public int getPrimitivePosition() {
    return primitivePosition;
  }
}
