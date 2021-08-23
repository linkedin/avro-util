/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

import java.util.Map;


public class FieldBuilder18 implements FieldBuilder {
  private final String _name;
  private Schema _schema;
  private String _doc;
  private JsonNode _defaultVal;
  private Order _order;
  private Map<String, JsonNode> _props;

  public FieldBuilder18(Schema.Field field) {
    this(field.name());
    _schema = field.schema();
    _doc = field.doc();
    //noinspection deprecation
    _defaultVal = field.defaultValue(); //deprecated but faster
    _order = field.order();
    //this is actually faster
    //noinspection deprecation
    _props = field.getJsonProps();
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
  public FieldBuilder setDefault(Object defaultValue, Schema schema) {
    if (defaultValue == null && AvroSchemaUtil.isUnionTypeWithNullAsFirstOption(schema)) {
      return setDefault(NullNode.getInstance());
    }
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
    @SuppressWarnings("deprecation") //deprecated but faster
    Schema.Field result = new Schema.Field(_name, _schema, _doc, _defaultVal, _order);
    if (_props != null) {
      for (Map.Entry<String, JsonNode> entry : _props.entrySet()) {
        //noinspection deprecation
        result.addProp(entry.getKey(), entry.getValue()); //deprecated but faster
      }
    }
    return result;
  }
}
