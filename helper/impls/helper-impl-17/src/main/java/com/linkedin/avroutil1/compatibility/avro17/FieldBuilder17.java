/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.JsonNode;

import java.util.Map;


public class FieldBuilder17 implements FieldBuilder {
  private final String _name;
  private Schema _schema;
  private String _doc;
  private JsonNode _defaultVal;
  private Order _order;
  private Map<String, JsonNode> _props;

  public FieldBuilder17(Schema.Field field) {
    this(field.name());
    _schema = field.schema();
    _doc = field.doc();
    _defaultVal = field.defaultValue();
    _order = field.order();
    _props = Avro17Utils.getProps(field);
  }

  public FieldBuilder17(String name) {
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
  @Deprecated
  public FieldBuilder copyFromField() {
    return this;
  }

  @Override
  public Schema.Field build() {
    Schema.Field result = new Schema.Field(_name, _schema, _doc, _defaultVal, _order);
    if (_props != null) {
      Avro17Utils.setProps(result, _props);
    }
    return result;
  }
}
