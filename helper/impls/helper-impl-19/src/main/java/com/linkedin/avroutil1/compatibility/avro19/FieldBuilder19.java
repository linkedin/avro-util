/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;

import java.util.Map;


public class FieldBuilder19 implements FieldBuilder {
  private final String _name;
  private Schema _schema;
  private String _doc;
  private Object _defaultVal;
  private Order _order;
  private Map<String, Object> _props;

  public FieldBuilder19(Schema.Field field) {
    this(field.name());
    _schema = field.schema();
    _doc = field.doc();
    _defaultVal = field.defaultVal();
    _order = field.order();
    _props = field.getObjectProps();
  }

  public FieldBuilder19(String name) {
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
  public FieldBuilder setDefault(Object defaultValue, Schema schema) {
    if (defaultValue == null && AvroSchemaUtil.isUnionTypeWithNullAsFirstOption(schema)) {
      defaultValue = Schema.Field.NULL_DEFAULT_VALUE;
    }
    _defaultVal = defaultValue;
    return this;
  }

  @Override
  public FieldBuilder setOrder(Order order) {
    _order = order;
    return this;
  }

  @Override
  @Deprecated
  public FieldBuilder copyFromField() {
    return this;
  }

  @Override
  public Schema.Field build() {
    Schema.Field result = new Schema.Field(_name, _schema, _doc, _defaultVal, _order);
    if (_props != null) {
      for (Map.Entry<String, Object> entry : _props.entrySet()) {
        result.addProp(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }
}
