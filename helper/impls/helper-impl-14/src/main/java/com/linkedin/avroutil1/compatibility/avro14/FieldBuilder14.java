/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.NullNode;


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

  private String _name;
  private Schema _schema;
  private String _doc;
  private JsonNode _defaultVal;
  private Order _order = Order.ASCENDING;
  private Map<String,String> _props;

  public FieldBuilder14(Schema.Field other) {
    if (other != null) {
      _name = other.name();
      _schema = other.schema();
      _doc = other.doc();
      _defaultVal = other.defaultValue();
      _order = other.order();
      if (_order == null) {
        // If the other field was created directly through Avro 1.4 APIs, it could have a null order.
        _order = Order.ASCENDING;
      }
      _props = getProps(other);
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
    if (order == null) {
      throw new IllegalArgumentException("sort order cannot be null");
    }
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

  @Override
  public FieldBuilder addProp(String propName, String jsonLiteral) {
    if (propName == null || jsonLiteral == null) {
      throw new IllegalArgumentException("Function input parameters cannot be null.");
    }
    if (_props == null) {
      _props = new HashMap<>();
    }
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      JsonNode jsonNode = objectMapper.readTree(jsonLiteral);
      if (!jsonNode.isTextual()) {
        throw new IllegalArgumentException("In Avro 1.4, can only use textual values, not " + jsonLiteral);
      }
      _props.put(propName, jsonNode.getTextValue());
      return this;
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse serialized json object: " + jsonLiteral, e);
    }
  }

  @Override
  public FieldBuilder addProps(Map<String, String> propNameToJsonObjectMap) {
    if (propNameToJsonObjectMap == null) {
      throw new IllegalArgumentException("Function input parameters cannot be null.");
    }
    for (Map.Entry<String, String> entry : propNameToJsonObjectMap.entrySet()) {
      try {
        addProp(entry.getKey(), entry.getValue());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Issue with adding prop with key: " + entry.getKey() + " and value: " + entry.getValue(), e);
      }
    }
    return this;
  }

  @Override
  public FieldBuilder removeProp(String propName) {
    if (propName == null || _props == null || !_props.containsKey(propName)) {
      throw new IllegalArgumentException("Cannot remove prop that doesn't exist: " + propName);
    }
    _props.remove(propName);
    return null;
  }
}
