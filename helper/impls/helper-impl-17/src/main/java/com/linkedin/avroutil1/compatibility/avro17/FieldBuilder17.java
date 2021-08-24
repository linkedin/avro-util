/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.NullNode;

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
    if (_defaultVal == NullNode.getInstance()) {
      // Check if null is still a valid default for the schema.
      setDefault((Object) null);
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
    Schema.Field result = new Schema.Field(_name, _schema, _doc, _defaultVal, _order);
    if (_props != null) {
      Avro17Utils.setProps(result, _props);
    }
    return result;
  }
}
