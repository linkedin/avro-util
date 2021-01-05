/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.linkedin.avroutil1.compatibility.FieldBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;


public class FieldBuilder19 implements FieldBuilder {
  private final Schema.Field _field;
  private Schema _schema;
  private String _doc;
  private Object _defaultVal;
  private Order _order;

  public FieldBuilder19(Schema.Field field) {
    _field = field;
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
    _defaultVal = _field.defaultVal();
    _order = _field.order();
    return this;
  }

  @Override
  public Schema.Field build() {
    return new Schema.Field(_field.name(), _schema, _doc, _defaultVal, _order);
  }
}
