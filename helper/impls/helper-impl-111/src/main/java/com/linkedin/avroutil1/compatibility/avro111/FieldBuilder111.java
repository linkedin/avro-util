/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;

import java.util.Map;


public class FieldBuilder111 implements FieldBuilder {
  private String _name;
  private Schema _schema;
  private String _doc;
  private Object _defaultVal;
  private Order _order;
  private Map<String, Object> _props;

  public FieldBuilder111(Schema.Field other) {
    if (other != null) {
      _name = other.name();
      _schema = other.schema();
      _doc = other.doc();
      _defaultVal = other.defaultVal();
      _order = other.order();
      _props = other.getObjectProps();
    }
  }

  @Override
  public FieldBuilder setName(String name) {
    _name = name;
    return this;
  }

  @Override
  public FieldBuilder setSchema(Schema schema) {
    _schema = schema;
    if (_defaultVal == Schema.Field.NULL_DEFAULT_VALUE) {
      // Check if null is still a valid default for the schema.
      setDefault(null);
    }
    return this;
  }

  @Override
  public FieldBuilder setDoc(String doc) {
    _doc = doc;
    return this;
  }

  @Override
  public FieldBuilder setDefault(Object defaultValue) {
    // If defaultValue is null, it's ambiguous. It could mean either of these:
    // (1) The default value was not specified, or
    // (2) The default value was specified to be null.
    //
    // To disambiguate, we check to see if null is a valid value for the
    // field's schema. If it is, we convert it into a special object (marker)
    // that's known to Avro. If it's not, it's case (1); we leave it as is.
    //
    // This means there's no way (using the helper) to create a field whose
    // schema allows null as a default, but you want to say "no default was
    // specified". That's a small price to pay for not bloating the helper API.
    //
    // Note that we don't validate all possible default values against the
    // schema. That's Avro's job. We only check for the ambiguous case here.
    if (defaultValue == null && AvroSchemaUtil.isNullAValidDefaultForSchema(_schema)) {
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
    Object avroFriendlyDefault;
    try {
      avroFriendlyDefault = AvroSchemaUtil.avroFriendlyDefaultValue(_defaultVal);
    } catch (Exception e) {
      throw new IllegalArgumentException("unable to convert default value " + _defaultVal + " into something avro can handle", e);
    }
    Schema.Field result = new Schema.Field(_name, _schema, _doc, avroFriendlyDefault, _order);
    if (_props != null) {
      for (Map.Entry<String, Object> entry : _props.entrySet()) {
        result.addProp(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }
}
