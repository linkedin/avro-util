/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class FixDefaultValueStep extends AbstractSchemaTraversingStep {

  public static class BadDefaultPropertySpec {
    private final String _fieldName;
    private final String _badLiteral;
    private final String _requiredType;

    public BadDefaultPropertySpec(String fieldName, String badLiteral, String requiredType) {
      _fieldName = fieldName;
      _badLiteral = badLiteral;
      _requiredType = requiredType;
    }

    @Override
    public String toString() {
      return _requiredType + " " + _fieldName + " = " + _badLiteral;
    }
  }

  private enum ValueType {
    INT, STRING, BOOLEAN;

    static ValueType fromLiteral(String literal) {
      if (literal.startsWith("\"") && literal.endsWith("\"")) {
        return STRING;
      }
      if (literal.equalsIgnoreCase("true") || literal.equalsIgnoreCase("false")) {
        return BOOLEAN;
      }
      try {
        //noinspection ResultOfMethodCallIgnored
        Integer.parseInt(literal);
        return INT;
      } catch (NumberFormatException ignored) {
        //empty
      }
      throw new IllegalStateException("unhandled: " + literal);
    }

    static ValueType fromAvroType(String avroType) {
      switch (avroType) {
        case "int":
          return INT;
        case "boolean":
          return BOOLEAN;
        case "string":
          return STRING;
        default:
          throw new IllegalStateException("unhandled: " + avroType);
      }
    }

    static ValueType fromDOMValue(Object value) {
      if (value instanceof String) {
        return STRING;
      }
      if (value instanceof Integer) {
        return INT;
      }
      if (value instanceof Boolean) {
        return BOOLEAN;
      }
      throw new IllegalStateException("unhandled: " + String.valueOf(value));
    }

    Object convert(Object domValue) {
      switch (this) {
        case INT:
          try {
            return Integer.parseInt(String.valueOf(domValue));
          } catch (NumberFormatException e) {
            return null; //cannot convert
          }
        case STRING:
          return String.valueOf(domValue);
        case BOOLEAN:
          String strVal = String.valueOf(domValue);
          if ("true".equalsIgnoreCase(strVal)) {
            return Boolean.TRUE;
          }
          if ("false".equalsIgnoreCase(strVal)) {
            return Boolean.TRUE;
          }
          return null; //cannot convert
        default:
          throw new IllegalStateException("unhandled: " + this);
      }
    }
  }

  private final BadDefaultPropertySpec _propertySpec;
  private final ValueType _badLiteralType;
  private final ValueType _expectedType;

  public FixDefaultValueStep(Exception cause, BadDefaultPropertySpec propertySpec) {
    super(cause);
    _propertySpec = propertySpec;
    _badLiteralType = ValueType.fromLiteral(_propertySpec._badLiteral);
    _expectedType = ValueType.fromAvroType(_propertySpec._requiredType);
  }

  @Override
  public SchemaTransformStep inverse() {
    return new AbstractSchemaTransformStep(_cause) {

      @Override
      public String applyToText(String text) {
        return text; //nop
      }

      @Override
      public SchemaTransformStep inverse() {
        return FixDefaultValueStep.this;
      }
    };
  }

  @Override
  public String toString() {
    return _propertySpec._fieldName + " = " + _propertySpec._badLiteral + " (" + _badLiteralType + ") --> " + _expectedType;
  }

  @Override
  protected void visitRecordObject(JSONObject recordObject, List<String> path) throws JSONException {
    //go over all fields looking for our target field.
    //if found, try converting default value to correct type.
    JSONArray fieldsArray = recordObject.getJSONArray("fields");
    for (int i = 0; i < fieldsArray.length(); i++) {
      JSONObject fieldObject = fieldsArray.getJSONObject(i);
      String fieldName = fieldObject.getString("name");
      if (fieldName.equals(_propertySpec._fieldName) && fieldObject.has("default")) {
        Object defaultValue = fieldObject.get("default");
        ValueType valueType = ValueType.fromDOMValue(defaultValue);
        if (_badLiteralType.equals(valueType)) {
          Object fixedDefaultValue = _expectedType.convert(defaultValue);
          if (fixedDefaultValue != null) {
            fieldObject.put("default", fixedDefaultValue);
          }
        }
      }
    }
  }
}
