/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.compatibility.HelperConsts;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaVisitor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;


public class Avro16SchemaValidator implements SchemaVisitor {
  private final static Map<Schema.Type, List<JsonParser.NumberType>> STRICT_JSON_NUMERIC_TYPES_PER_AVRO_TYPE;
  private final static Map<Schema.Type, List<JsonParser.NumberType>> LOOSE_JSON_NUMERIC_TYPES_PER_AVRO_TYPE;

  static {
    Map<Schema.Type, List<JsonParser.NumberType>> strict = new HashMap<>(4);
    Map<Schema.Type, List<JsonParser.NumberType>> loose = new HashMap<>(4);

    List<JsonParser.NumberType> allFloats = Collections.unmodifiableList(Arrays.asList(
        JsonParser.NumberType.FLOAT, JsonParser.NumberType.DOUBLE
    ));
    List<JsonParser.NumberType> allNumerics = Collections.unmodifiableList(Arrays.asList(
        JsonParser.NumberType.INT, JsonParser.NumberType.LONG, JsonParser.NumberType.FLOAT, JsonParser.NumberType.DOUBLE
    ));

    strict.put(Schema.Type.INT, Collections.singletonList(JsonParser.NumberType.INT));
    strict.put(Schema.Type.LONG, Collections.unmodifiableList(Arrays.asList(JsonParser.NumberType.INT, JsonParser.NumberType.LONG)));
    //jackson (used by avro) seems to like parsing everything as DoubleNode
    strict.put(Schema.Type.FLOAT, allFloats);
    strict.put(Schema.Type.DOUBLE, allFloats);

    loose.put(Schema.Type.INT, Collections.unmodifiableList(Arrays.asList(JsonParser.NumberType.INT, JsonParser.NumberType.FLOAT, JsonParser.NumberType.DOUBLE)));
    loose.put(Schema.Type.LONG, allNumerics);
    loose.put(Schema.Type.FLOAT, allNumerics);
    loose.put(Schema.Type.DOUBLE, allNumerics);

    STRICT_JSON_NUMERIC_TYPES_PER_AVRO_TYPE = Collections.unmodifiableMap(strict);
    LOOSE_JSON_NUMERIC_TYPES_PER_AVRO_TYPE = Collections.unmodifiableMap(loose);
  }

  private final SchemaParseConfiguration validationSpec;
  private final Collection<Schema> grandfathered;

  /**
   * constructs a new validator
   * @param validationSpec determines what should be validated
   * @param grandfathered a set of schemas to be excluded from validation (if encountered)
   */
  public Avro16SchemaValidator(SchemaParseConfiguration validationSpec, Collection<Schema> grandfathered) {
    if (validationSpec == null) {
      throw new IllegalArgumentException("validationSpec required");
    }
    this.validationSpec = validationSpec;
    this.grandfathered = grandfathered != null && !grandfathered.isEmpty() ? grandfathered : Collections.emptySet();
  }

  @Override
  public void visitSchema(Schema schema) {
    if (grandfathered.contains(schema)) {
      return;
    }
    if (!validationSpec.validateNames()) {
      return;
    }
    Schema.Type type = schema.getType();
    if (!HelperConsts.NAMED_TYPES.contains(type)) {
      return;
    }
    //TODO - avro only validates the SIMPLE name, so for now so do we.
    //see https://issues.apache.org/jira/browse/AVRO-2742
    String simpleName = schema.getName();
    validateName(simpleName, " in " + type.name().toLowerCase(Locale.ROOT) + " " + schema.getFullName());
    if (type == Schema.Type.ENUM) {
      List<String> symbols = schema.getEnumSymbols();
      for (String symbol : symbols) {
        validateName(symbol, " in " + type.name().toLowerCase(Locale.ROOT) + " " + schema.getFullName());
      }
    }
  }

  @Override
  public void visitField(Schema parent, Schema.Field field) {
    if (grandfathered.contains(parent)) {
      return;
    }
    if (validationSpec.validateNames()) {
      String fieldName = field.name();
      validateName(fieldName, " in field " + parent.getFullName() + "." + fieldName);
    }
    JsonNode defaultValue = field.defaultValue();
    if (validationSpec.validateDefaultValues() && defaultValue != null) {
      Schema fieldSchema = field.schema();
      boolean validDefault = isValidDefault(fieldSchema, defaultValue, validationSpec.validateNumericDefaultValueTypes());
      if (!validDefault) {
        //throw ~the same exception avro would
        String message = "Invalid default for field " + parent.getFullName() + "." + field.name() + ": "
            + defaultValue + " (a " + defaultValue.getClass().getSimpleName() + ") not a " + fieldSchema;
        throw new AvroTypeException(message);
      }
    }
  }

  /**
   * validation logic taken out of class {@link Schema} with adaptations
   * @param name name to be validated
   * @throws SchemaParseException is name is invalid
   */
  private static void validateName(String name, String suffix) {
    int length = name.length();
    if (length == 0) {
      throw new SchemaParseException("Empty name" + suffix);
    }
    char first = name.charAt(0);
    if (!(Character.isLetter(first) || first == '_')) {
      throw new SchemaParseException("Illegal initial character: " + name + suffix);
    }
    for (int i = 1; i < length; i++) {
      char c = name.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_')) {
        throw new SchemaParseException("Illegal character in: " + name + " ('" + c + "' at position " + i + ")" + suffix);
      }
    }
  }

  /**
   * validation logic taken out of class {@link Schema} with adaptations
   * @param schema schema (type) of a field
   * @param defaultValue default value provided for said field in the parent schema
   * @param validateNumericTypes true to use strict numeric type matching between value and schema
   * @throws SchemaParseException is name is invalid
   */
  public static boolean isValidDefault(Schema schema, JsonNode defaultValue, boolean validateNumericTypes) {
    if (defaultValue == null) {
      //means no default value
      return false;
    }
    Schema.Type avroType = schema.getType();
    switch (avroType) {
      case STRING:
      case BYTES:
      case ENUM:
      case FIXED:
        return defaultValue.isTextual();
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        Map<Schema.Type, List<JsonParser.NumberType>> lookupTable = validateNumericTypes ?
            STRICT_JSON_NUMERIC_TYPES_PER_AVRO_TYPE : LOOSE_JSON_NUMERIC_TYPES_PER_AVRO_TYPE;
        List<JsonParser.NumberType> allowedTypes = lookupTable.get(avroType);
        if (allowedTypes == null || !allowedTypes.contains(defaultValue.getNumberType())) {
          return false;
        }
        if (avroType == Schema.Type.INT || avroType == Schema.Type.LONG) {
          //dont allow true non-round numbers for ints
          if (!Jackson1Utils.isRoundNumber(defaultValue)) {
            return false;
          }
        }
        //TODO - check values out of range (like 5*MAX_INT for int field)
        return true;
      case BOOLEAN:
        return defaultValue.isBoolean();
      case NULL:
        return defaultValue.isNull();
      case ARRAY:
        if (!defaultValue.isArray()) {
          return false;
        }
        for (JsonNode element : defaultValue) {
          if (!isValidDefault(schema.getElementType(), element, validateNumericTypes)) {
            return false;
          }
        }
        return true;
      case MAP:
        if (!defaultValue.isObject()) {
          return false;
        }
        for (JsonNode value : defaultValue) {
          if (!isValidDefault(schema.getValueType(), value, validateNumericTypes)) {
            return false;
          }
        }
        return true;
      case UNION: // union default: first branch
        return isValidDefault(schema.getTypes().get(0), defaultValue, validateNumericTypes);
      case RECORD:
        if (!defaultValue.isObject()) {
          return false;
        }
        for (Schema.Field field : schema.getFields()) {
          if (!isValidDefault(
              field.schema(),
              defaultValue.get(field.name()) != null ? defaultValue.get(field.name()) : field.defaultValue(),
              validateNumericTypes
          )) {
            return false;
          }
        }
        return true;
      default:
        return false;
    }
  }
}
