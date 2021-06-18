/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.JsonNode;

import java.lang.reflect.Field;
import java.util.Map;


public class FieldBuilder14 implements FieldBuilder {
  private final static Field SCHEMA_FIELD_PROPS_FIELD;

  static {
    try {
      Class<Schema.Field> fieldClass = Schema.Field.class;
      SCHEMA_FIELD_PROPS_FIELD = fieldClass.getDeclaredField("props");
      SCHEMA_FIELD_PROPS_FIELD.setAccessible(true); //muwahahaha
    } catch (Throwable issue) {
      throw new IllegalStateException("unable to find/access Schema$Field.props", issue);
    }
  }

  private final String _name;
  private Schema _schema;
  private String _doc;
  private JsonNode _defaultVal;
  private Order _order;
  private Map<String,String> _props;

  public FieldBuilder14(Schema.Field field) {
    this(field.name());
    _schema = field.schema();
    _doc = field.doc();
    _defaultVal = field.defaultValue();
    _order = field.order();
    _props = getProps(field);
  }

  public FieldBuilder14(String name) {
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
    if (_props != null && !_props.isEmpty()) {
      Map<String, String> clonedProps = getProps(result);
      clonedProps.putAll(_props);
    }
    return result;
  }

  private Map<String,String> getProps(Schema.Field field) {
    try {
      @SuppressWarnings("unchecked")
      Map<String, String> props = (Map<String, String>) SCHEMA_FIELD_PROPS_FIELD.get(field);
      return props;
    } catch (Exception e) {
      throw new IllegalStateException("unable to access props on Schema$Field " + field.name(), e);
    }
  }
}
