/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.JsonNode;


public class FieldBuilder18 implements FieldBuilder {
  private final String _name;
  private Schema.Field _field;
  private Schema _schema;
  private String _doc;
  private JsonNode _defaultVal;
  private Order _order;

  public FieldBuilder18(Schema.Field field) {
    this(field.name());
    _field = field;
  }

  public FieldBuilder18(String name) {
    _name = name;
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

  @Override
  public FieldBuilder setDefault(Object defaultValue) {
    return setDefault(Jackson1Utils.toJsonNode(defaultValue));
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
  public FieldBuilder copyFromField() {
    if (_field == null) {
      throw new NullPointerException("Field in FieldBuilder can not be empty!");
    }
    _doc = _field.doc();
    _defaultVal = _field.defaultValue();
    _order = _field.order();
    return this;
  }

  @Override
  public Schema.Field build() {
    return new Schema.Field(_name, _schema, _doc, _defaultVal, _order);
  }
}
