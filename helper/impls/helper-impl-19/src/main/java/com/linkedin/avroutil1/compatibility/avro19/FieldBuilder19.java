/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import java.util.HashMap;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;

import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;


public class FieldBuilder19 implements FieldBuilder {
  private String _name;
  private Schema _schema;
  private String _doc;
  private Object _defaultVal;
  private Order _order = Order.ASCENDING;
  private Map<String, Object> _props;

  public FieldBuilder19(Schema.Field other) {
    if (other != null) {
      _name = other.name();
      _schema = other.schema();
      _doc = other.doc();
      _defaultVal = other.defaultVal();
      _order = other.order();  // other.order() cannot be null under Avro 1.9.*
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
    Object avroFriendlyDefault;
    try {
      avroFriendlyDefault = avroFriendlyDefaultValue(_defaultVal);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "unable to convert default value " + _defaultVal + " into something avro can handle", e);
    }
    Schema.Field result = new Schema.Field(_name, _schema, _doc, avroFriendlyDefault, _order);
    if (_props != null) {
      for (Map.Entry<String, Object> entry : _props.entrySet()) {
        result.addProp(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  @Override
  public FieldBuilder addProp(String propName, String jsonLiteral) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      _props.put(propName, objectMapper.readTree(jsonLiteral));
      return this;
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse serialized json object: " + jsonLiteral, e);
    }
  }

  @Override
  public FieldBuilder addProps(Map<String, String> propNameToJsonObjectMap) {
    for (Map.Entry<String, String> entry : propNameToJsonObjectMap.entrySet()) {
      addProp(entry.getKey(), entry.getValue());
    }
    return this;
  }

  @Override
  public FieldBuilder removeProp(String propName) {
    if (!_props.containsKey(propName)) {
      throw new IllegalStateException("Cannot remove prop that doesn't exist: " + propName);
    }
    _props.remove(propName);
    return null;
  }

  /**
   * we want to be very generous with what we let users provide for default values.
   * sadly, (modern) avro can only handle specific classes/collections/primitive-wrappers
   * (see org.apache.avro.util.internal.JacksonUtils.toJson(Object, JsonGenerator) in 1.9+)
   * @param mightNotBeFriendly a proposed field default value that might originate from
   *                           a call like AvroCompatibilityHelper.getGenericDefaultValue()
   * @return a representation of the input that avro likes for use as a field default value
   */
  private static Object avroFriendlyDefaultValue(Object mightNotBeFriendly) throws Exception {

    //generic enums we turn to strings
    if (mightNotBeFriendly instanceof GenericData.EnumSymbol) {
      return mightNotBeFriendly.toString(); // == symbol string
    }

    //fixed (generic or specific) we turn to bytes
    if (mightNotBeFriendly instanceof GenericFixed) {
      return ((GenericFixed) mightNotBeFriendly).bytes();
    }

    //records (generic or specific) we turn to maps
    if (mightNotBeFriendly instanceof IndexedRecord) {
      IndexedRecord record = (IndexedRecord) mightNotBeFriendly;
      Schema recordSchema = record.getSchema();

      Map<String, Object> map = new HashMap<>();
      for (Schema.Field field : recordSchema.getFields()) {
        Object fieldValue = record.get(field.pos());
        if (fieldValue == null) {
          fieldValue = JsonProperties.NULL_VALUE;
        } else {
          fieldValue = avroFriendlyDefaultValue(fieldValue);
        }
        map.put(field.name(), fieldValue);
        //TODO - extra props ?!
      }
      return map;
    }
    return mightNotBeFriendly;
  }
}
