/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import com.linkedin.avroutil1.compatibility.FieldBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.JsonNode;


public class FieldBuilder15 implements FieldBuilder {
  private static Schema.Field _field;
  private Schema _schema;
  private String _doc;
  private JsonNode _defaultVal;
  private Order _order;

  @Override
  public FieldBuilder setField(Schema.Field field) {
    _field = field;
    return this;
  }

  @Override
  public FieldBuilder setSchema(Schema schema) {
    _schema = schema;
    return this;
  }

  @Override
  public FieldBuilder setDoc(String doc) {
    _doc = doc;
    return this;
  }

  public FieldBuilder setDefault(Object defaultValue) {
    return null;
  }

  public FieldBuilder setDefault(JsonNode defaultValue) {
    _defaultVal = defaultValue;
    return this;
  }

  @Override
  public FieldBuilder setOrder(Order order) {
    _order = order;
    return this;
  }

  @Override
  public FieldBuilder copyFromField(Schema.Field field) {
    _field = field;
    _doc = field.doc();
    _defaultVal = field.defaultValue();
    _order = field.order();
    return this;
  }

  @Override
  public Schema.Field build() {
    return new Schema.Field(_field.name(), _schema, _doc, _defaultVal, _order);
  }
}
