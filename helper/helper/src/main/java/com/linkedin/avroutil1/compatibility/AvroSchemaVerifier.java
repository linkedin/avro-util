/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.codehaus.jackson.JsonNode;

/**
 * Class that will take two avro schema and see if they are backwards compatible.
 * It verifies that the schema's union field default value must be same type as the first field.
 *     From https://avro.apache.org/docs/current/spec.html#Unions
 *     (Note that when a default value is specified for a record field whose type is a union, the type of the
 *     default value must match the first element of the union. Thus, for unions containing "null", the "null" is usually
 *     listed first, since the default value of such unions is typically null.)
 * @author Richard Park
 */
public class AvroSchemaVerifier {
  private final static AvroSchemaVerifier INSTANCE = new AvroSchemaVerifier();
  private final static boolean IS_AVRO_VERSION_SUPPORTED;

  static {
    // Skip for Avro 1.9+ versions, as schema field.defaultValue() would throw
    // java.lang.NoSuchMethodError: org.apache.avro.Schema$Field.defaultValue()Lorg/codehaus/jackson/JsonNode exception.
    IS_AVRO_VERSION_SUPPORTED = AvroCompatibilityHelper.getRuntimeAvroVersion().earlierThan(AvroVersion.AVRO_1_9);
  }

  /**
   * Retrieves the verifier instance
   * @return
   */
  public static AvroSchemaVerifier get() {
    return INSTANCE;
  }

  /**
   * Hidden constructor
   */
  private AvroSchemaVerifier() {
  }

  /**
   * Verify the old schema with the new. Will throw and exception if there's incompatibilities found.
   * Otherwise, it will pass silently.
   *
   * @param oldSchema The old schema
   * @param newSchema The new schema
   * @throws AvroIncompatibleSchemaException
   */
  public void verifyCompatibility(Schema oldSchema, Schema newSchema) throws AvroIncompatibleSchemaException {
    if (!IS_AVRO_VERSION_SUPPORTED) {
      return;
    }
    recurseSchema(oldSchema, newSchema, "", new HashSet<Schema>());
  }

  /**
   * Used to recurse the schemas and match them together.
   *
   * @param oldSchema
   * @param newSchema
   * @param parent
   * @throws AvroIncompatibleSchemaException
   */
  private void recurseSchema(Schema oldSchema, Schema newSchema, String parent, HashSet<Schema> visitedSchema)
      throws AvroIncompatibleSchemaException {
    Schema.Type oldSchemaType = oldSchema.getType();
    Schema.Type newSchemaType = newSchema.getType();

    String message = null;
    switch (newSchemaType) {
      case NULL:
      case BOOLEAN:
      case BYTES:
      case INT:
      case STRING:
        if (oldSchemaType == newSchemaType) {
          return;
        }
        break;
      case LONG:
        switch (oldSchemaType) {
          case LONG:
          case INT:
            return;
          default:
            break;
        }
        break;
      case FLOAT:
        switch (oldSchemaType) {
          case FLOAT:
          case INT:
          case LONG:
            return;
          default:
            break;
        }
        break;
      case DOUBLE:
        switch (oldSchemaType) {
          case DOUBLE:
          case FLOAT:
          case INT:
          case LONG:
            return;
          default:
            break;
        }
        break;
      case FIXED:
        if (oldSchemaType == newSchemaType) {
          if (oldSchema.getFixedSize() == newSchema.getFixedSize()) {
            return;
          }
          message = "Fixed type size mismatch";
        }
        break;
      case ENUM:
        // Enum types must match
        if (oldSchemaType == newSchemaType) {
          if (oldSchema.getFullName().equals(newSchema.getFullName())) {
            boolean isGood = true;
            // Check that no old enums were removed and that the ordinal for enum symbols are the same
            for (String oldEnumSymbol : oldSchema.getEnumSymbols()) {
              if (!newSchema.hasEnumSymbol(oldEnumSymbol)
                  || newSchema.getEnumOrdinal(oldEnumSymbol) != oldSchema.getEnumOrdinal(oldEnumSymbol)) {
                isGood = false;
                break;
              }
            }

            if (isGood) {
              return;
            }
            message = "Enums are missing from the new schema or enums have been reordered.";
          } else {
            message = String.format("%s %s changed to %s %s", oldSchemaType, oldSchema.getFullName(), newSchemaType, newSchema.getFullName());
          }
        } else {
          //getFullName not defined for all types, so if both types are not enum, use just names
          message = String.format("%s %s changed to %s %s", oldSchemaType, oldSchema.getName(), newSchemaType, newSchema.getName());
        }
        break;
      case MAP:
        if (oldSchemaType == newSchemaType) {
          recurseSchema(oldSchema.getValueType(), newSchema.getValueType(), parent + ":" + newSchema.getName(),
              visitedSchema);
          return;
        }
        break;
      case ARRAY:
        if (oldSchemaType == newSchemaType) {
          recurseSchema(oldSchema.getElementType(), newSchema.getElementType(), parent + ":" + newSchema.getName(),
              visitedSchema);
          return;
        }
        break;
      case RECORD:
        // The record types much match.
        if (oldSchemaType == Schema.Type.RECORD && oldSchema.getFullName().equals(newSchema.getFullName())) {
          resolveRecord(oldSchema, newSchema, parent + ":" + newSchema.getName(), visitedSchema);
          return;
        }
        break;
      case UNION:
        if (resolveUnion(oldSchema, newSchema, visitedSchema)) {
          return;
        }

        message = "Compatible versions not found in union";
        break;
      default:
        message = "Unsupported type: " + newSchemaType;
        break;
    }

    if (message == null) {
      message = "Type mismatch old schema " + oldSchemaType + " to new " + newSchemaType;
      message += "\nOld Schema:" + oldSchema;
      message += "\nNew Schema:" + newSchema;
    }
    throw new AvroIncompatibleSchemaException(message + " at " + parent + ":" + newSchema.getName());
  }

  /**
   * Used to resolve unions
   *
   * @param oldSchema
   * @param newUnion
   * @return
   */
  private boolean resolveUnion(Schema oldSchema, Schema newUnion, HashSet<Schema> visitedSchema) {
    if (oldSchema.getType() == Schema.Type.UNION) {
      for (Schema schema : oldSchema.getTypes()) {
        if (!resolveUnion(schema, newUnion, visitedSchema)) {
          return false;
        }
      }

      return true;
    } else {
      for (Schema schema : newUnion.getTypes()) {
        try {
          recurseSchema(oldSchema, schema, "", visitedSchema);
          return true;
        } catch (AvroIncompatibleSchemaException e) {
          //e.printStackTrace();
        }
      }

      return false;
    }
  }

  /**
   * Used to resolve records
   *
   * @param oldRecord
   * @param newRecord
   * @param parent
   * @throws AvroIncompatibleSchemaException
   */
  private void resolveRecord(Schema oldRecord, Schema newRecord, String parent, HashSet<Schema> visitedSchema)
      throws AvroIncompatibleSchemaException {
    if (visitedSchema.contains(newRecord)) {
      //System.out.println("Record " + newRecord.getName() + " has been processed. Will not recurse.");
      return;
    }

    visitedSchema.add(newRecord);

    // Retrieve all the old fields.
    HashMap<String, Field> oldFieldMap = new HashMap<String, Field>();
    for (Field field : oldRecord.getFields()) {
      oldFieldMap.put(field.name(), field);
    }

    // Check to see if the field exists in the old record. If not, then the default must be set.
    for (Field field : newRecord.getFields()) {
      Field oldField = oldFieldMap.get(field.name());
      if (oldField != null) {
        // Check that if the old field was set, then the new field would be set too.
        if (oldField.defaultValue() != null && field.defaultValue() == null) {
          throw new AvroIncompatibleSchemaException(
              "Field " + field.name() + " in " + parent + " has a missing default value.");
        } else if (field.defaultValue() != null) {
          checkDefaultValue(newRecord, field);
        }

        recurseSchema(oldField.schema(), field.schema(), parent + ":" + field.name(), visitedSchema);
      } else {
        if (field.schema().getType() == Schema.Type.RECORD && field.schema().getAliases() != null) {
          // Check to see if an alias is found.
          for (String alias : field.schema().getAliases()) {
            oldField = oldFieldMap.get(alias);
            // Alias has been found
            if (oldField != null) {
              break;
            }
          }
        }

        if (oldField != null) {
          // A matching alias was found. Recurse on it.
          recurseSchema(oldField.schema(), field.schema(), parent + ":" + field.name(), visitedSchema);
        } else {
          // No matching schema is found, so we look for default.
          checkDefaultValue(newRecord, field);
        }
      }
    }

    // now check the other way to make sure that all old fields exist in the new schema
    HashMap<String, Field> newFieldMap = new HashMap<String, Field>();
    for (Field field : newRecord.getFields()) {
      newFieldMap.put(field.name(), field);
    }
    for (String fieldName : oldFieldMap.keySet()) {
      if (!newFieldMap.containsKey(fieldName) && !isWhitelisted(parent, fieldName)) {
        throw new AvroIncompatibleSchemaException("Field " + fieldName + " in " + parent
            + " existed in a previous version of the schema and has been removed.");
      }
    }
  }

  private boolean isWhitelisted(String parent, String fieldName) {
    if (parent.equals(":ConnectFrameworkImpressionEvent")) {
      return (fieldName.equals("firehoseMetadata") || fieldName.equals("joinedData"));
    }
    return false;
  }

  /**
   * Makes sure the default value is good
   *
   * @param parent
   * @param field
   * @throws AvroIncompatibleSchemaException
   */
  private void checkDefaultValue(Schema parent, Field field) throws AvroIncompatibleSchemaException {
    JsonNode defaultJson = field.defaultValue();

    if (defaultJson == null) {
      throw new AvroIncompatibleSchemaException(
          "Field " + parent.getName() + ":" + field.name() + " needs default value set in record.");
    }

    String expectedVal = checkDefaultJson(defaultJson, field.schema());

    if (expectedVal != null) {
      throw new AvroIncompatibleSchemaException(
          getDefaultValueMessage(parent.getName(), field.name(), expectedVal, defaultJson.toString()));
    }
  }

  /**
   * Check that the default json node is a valid default value
   *
   * @param defaultJson
   * @param schema
   * @return
   */
  private String checkDefaultJson(JsonNode defaultJson, Schema schema) {
    Schema.Type fieldType = schema.getType();
    String expectedVal = null;
    switch (fieldType) {
      case NULL:
        if (!defaultJson.isNull()) {
          expectedVal = "null";
        }

        break;
      case BOOLEAN:
        if (!defaultJson.isBoolean()) {
          expectedVal = "boolean";
        }
        break;
      case INT:
        if (!defaultJson.isInt()) {
          expectedVal = "int";
        }
        break;
      case LONG:
        if (!defaultJson.isInt() && !defaultJson.isLong()) {
          expectedVal = "long";
        }
        break;
      case FLOAT:
      case DOUBLE:
        if (!defaultJson.isNumber()) {
          expectedVal = "number";
        }
        break;
      case BYTES:
        if (defaultJson.isTextual()) {
          break;
        }
        expectedVal = "bytes (ex. \"\\u00FF\")";
        break;
      case STRING:
        if (!defaultJson.isTextual()) {
          expectedVal = "string";
        }
        break;
      case ENUM:
        if (defaultJson.isTextual()) {
          if (schema.hasEnumSymbol(defaultJson.getTextValue())) {
            break;
          }
        }
        expectedVal = "valid enum";
        break;
      case FIXED:
        if (defaultJson.isTextual()) {
          byte[] fixed = defaultJson.getValueAsText().getBytes();
          if (fixed.length == schema.getFixedSize()) {
            break;
          }
          expectedVal = "fixed size incorrect. Expected size: " + schema.getFixedSize() + " got size " + fixed.length;
          break;
        }
        expectedVal = "fixed (ex. \"\\u00FF\")";
        break;
      case ARRAY:
        if (defaultJson.isArray()) {
          // Check all array variables
          boolean isGood = true;
          for (JsonNode node : defaultJson) {
            String val = checkDefaultJson(node, schema.getElementType());
            if (val == null) {
              continue;
            } else {
              isGood = false;
              break;
            }
          }

          if (isGood) {
            break;
          }
        }
        expectedVal = "array of type " + schema.getElementType().toString();
        break;
      case MAP:
        if (defaultJson.isObject()) {
          boolean isGood = true;
          for (JsonNode node : defaultJson) {
            String val = checkDefaultJson(node, schema.getValueType());
            if (val == null) {
              continue;
            } else {
              isGood = false;
              break;
            }
          }

          if (isGood) {
            break;
          }
        }

        expectedVal = "map of type " + schema.getValueType().toString();
        break;
      case RECORD:
        if (defaultJson.isObject()) {
          boolean isGood = true;
          for (Field field : schema.getFields()) {
            JsonNode jsonNode = defaultJson.get(field.name());

            if (jsonNode == null) {
              jsonNode = field.defaultValue();
              if (jsonNode == null) {
                isGood = false;
                break;
              }
            }

            String val = checkDefaultJson(jsonNode, field.schema());
            if (val != null) {
              isGood = false;
              break;
            }
          }

          if (isGood) {
            break;
          }
        }

        expectedVal = "record of type " + schema.toString();
        break;
      case UNION:
        // Avro spec states we only need to match with the first item
        expectedVal = checkDefaultJson(defaultJson, schema.getTypes().get(0));
        break;
      default:
        break;
    }

    return expectedVal;
  }

  /**
   * Create a generic message for mismatched fields
   * @param parent
   * @param field
   * @param expected
   * @param value
   * @return
   */
  private String getDefaultValueMessage(String parent, String field, String expected, String value) {
    return "Field " + parent + ":" + field + " has invalid default value. Expecting " + expected + ", instead got "
        + value;
  }

  /**
   * Checks that the string value represents a hex value: i.e "\u00FF"
   *
   * @param str
   * @return
   */
  private boolean checkIfHexValue(String str) {
    char[] charArray = str.toCharArray();
    if (charArray.length < 3) {
      return false;
    }

    if (charArray[0] != '\\' && !(charArray[1] == 'u' || charArray[1] == 'U')) {
      return false;
    }

    for (int i = 2; i < charArray.length; ++i) {
      switch (charArray[i]) {
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case 'A':
        case 'a':
        case 'B':
        case 'b':
        case 'C':
        case 'c':
        case 'D':
        case 'd':
        case 'E':
        case 'e':
        case 'F':
        case 'f':
          break;
        default:
          return false;
      }
    }

    return true;
  }
}
