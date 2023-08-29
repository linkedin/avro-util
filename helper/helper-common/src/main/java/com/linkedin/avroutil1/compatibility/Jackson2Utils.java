/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.avro.Schema.Type.UNION;


public class Jackson2Utils {
  private static final JsonFactory JSON_FACTORY;
  private static final JsonFactory PERMISSIVE_JSON_FACTORY;
  private static final ObjectMapper OBJECT_MAPPER;
  private static final ObjectMapper PERMISSIVE_OBJECT_MAPPER;

  private static final Logger LOGGER = LoggerFactory.getLogger(Jackson2Utils.class);

  static {
    JSON_FACTORY = new JsonFactory();
    OBJECT_MAPPER = new ObjectMapper(JSON_FACTORY); //matches that used by avro
    JSON_FACTORY.setCodec(OBJECT_MAPPER);

    PERMISSIVE_JSON_FACTORY = new JsonFactory();
    PERMISSIVE_OBJECT_MAPPER = new ObjectMapper(PERMISSIVE_JSON_FACTORY);
    PERMISSIVE_JSON_FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
    PERMISSIVE_JSON_FACTORY.setCodec(PERMISSIVE_OBJECT_MAPPER);
  }

  private Jackson2Utils() {
    // Util class; should not be instantiated.
  }

  public static JsonNode toJsonNode(String str, boolean strict) {
    if (str == null) {
      return null;
    }
    try {
      JsonParser parser = JSON_FACTORY.createParser(str);
      JsonNode node = OBJECT_MAPPER.readTree(parser);
      if (node == null) {
        // Happens when str.isEmpty().
        throw new NullPointerException("value cannot be an empty string");
      }
      // Calling parser.nextToken() can raise an exception itself, if there are garbage chars at the end.
      // E.g., when str == "1,2", the ',' is the garbage char (raises JsonParseException).
      JsonToken token = parser.nextToken();
      if (token != null) {
        // Or, if that succeeds and returns a valid token, the extra chars (while not garbage) are still unwanted.
        // E.g., when str == "1 2", the '2' is the extra token.
        throw new IllegalStateException("unexpected token " + token + " at location " + parser.getTokenLocation()
            + "; the value must be a single literal without extra trailing chars");
      }
      return node;
    } catch (Exception issue) {
      if (strict) {
        throw new IllegalStateException("while trying to deserialize " + str, issue);
      }
    }
    return new TextNode(str);
  }

  public static String toJsonString(JsonNode node) {
    if (node == null) {
      return null;
    }
    try {
      return OBJECT_MAPPER.writeValueAsString(node);
    } catch (Exception issue) {
      throw new IllegalStateException("while trying to serialize " + node + " (a " + node.getClass().getName() + ")",
          issue);
    }
  }

  /**
   * compares 2 JsonNodes for equality, potentially allowing comparison of round floating point numbers and integers
   * of different types.
   * this implementation treats null as equals to null.
   * @param aVal a {@link JsonNode}, or null
   * @param bVal a {@link JsonNode}, or null
   * @param looseNumerics allow "loose" numeric comparisons (int node to long node, int nodes to round floating points etc)
   * @return true if both nodes are null or otherwise equal
   */
  public static boolean JsonNodesEqual(JsonNode aVal, JsonNode bVal, boolean looseNumerics) {
    if (aVal == null || bVal == null) {
      return aVal == null && bVal == null;
    }
    boolean numerics = aVal.isNumber() && bVal.isNumber(); //any cross-type comparison is going to be false anyway
    if (!numerics || !looseNumerics) {
      return Objects.equals(aVal, bVal);
    }
    //loose numerics
    if (aVal.isIntegralNumber()) {
      if (bVal.isIntegralNumber() || Jackson2Utils.isRoundNumber(bVal)) {
        //we dont care about numbers larger than 64 bit
        return aVal.longValue() == bVal.longValue();
      }
      return false; //b isnt round
    } else {
      //a is float
      if (!bVal.isIntegralNumber()) {
        //a and b are floats
        //this has issues with rounding and precision, but i'd rather stick to this until someone complains
        return aVal.doubleValue() == bVal.doubleValue();
      }
      //b is integral
      return Jackson2Utils.isRoundNumber(aVal) && (aVal.longValue() == bVal.longValue());
    }
  }

  public static boolean isRoundNumber(JsonNode node) {
    if (node == null || !node.isNumber()) {
      return false;
    }
    if (node.isIntegralNumber()) {
      return true;
    }
    double actual = node.asDouble();
    return Double.compare(Math.floor(actual), Math.ceil(actual)) == 0;
  }

  /**
   *  Enforces uniform numeric default values across Avro versions
   */
  public static JsonNode enforceUniformNumericDefaultValues(Schema.Field field, JsonNode genericDefaultValue) {
    Schema schema = field.schema();
    // a default value for a union, must match the first element of the union
    Schema.Type defaultType = schema.getType() == UNION ? schema.getTypes().get(0).getType() : schema.getType();

    switch (defaultType) {
      case INT:
        if (!isAMathematicalInteger(genericDefaultValue.decimalValue())) {
          LOGGER.warn(String.format("Invalid default value: %s for \"int\" field: %s", genericDefaultValue, field.name()));
          return genericDefaultValue;
        }
        return new IntNode(genericDefaultValue.intValue());
      case LONG:
        if (!isAMathematicalInteger(genericDefaultValue.decimalValue())) {
          LOGGER.warn(String.format("Invalid default value: %s for \"long\" field: %s", genericDefaultValue, field.name()));
          return genericDefaultValue;
        }
        return new LongNode(genericDefaultValue.longValue());
      case DOUBLE:
        return new DoubleNode(genericDefaultValue.doubleValue());
      case FLOAT:
        return new FloatNode(genericDefaultValue.floatValue());
      default:
        return genericDefaultValue;
    }
  }

  public static boolean compareJsonProperties(
      Map<String, JsonNode> jsonPropsA,
      Map<String, JsonNode> jsonPropsB,
      boolean compareStringProps,
      boolean compareNonStringProps
  ) {
    if (compareStringProps && compareNonStringProps) {
      return Objects.equals(jsonPropsA, jsonPropsB);
    }
    //compare all entries in A to B
    for (Map.Entry<String, JsonNode> aEnt : jsonPropsA.entrySet()) {
      String key = aEnt.getKey();
      JsonNode valueA = aEnt.getValue(); // != null
      JsonNode valueB = jsonPropsB.get(key); // might be null
      if (valueA.isTextual()) {
        if (compareStringProps && ! valueA.equals(valueB)) {
          return false;
        }
      } else {
        if (compareNonStringProps && !valueA.equals(valueB)) {
          return false;
        }
      }
    }
    //go over B looking for keys not in A, fail if any found.
    for (Map.Entry<String, JsonNode> bEnt : jsonPropsB.entrySet()) {
      String key = bEnt.getKey();
      JsonNode valueA = jsonPropsA.get(key); // null if no such key in A
      JsonNode valueB = bEnt.getValue(); // != null
      if (valueB.isTextual()) {
        if (compareStringProps && valueA == null) {
          return false;
        }
      } else {
        if (compareNonStringProps && valueA == null) {
          return false;
        }
      }
    }
    return true;
  }

  public static void assertNoTrailingContent(String json) {
    String dangling;
    JsonLocation endOfSchemaLocation;
    try {
      StringReader reader = new StringReader(json);
      JsonParser parser = PERMISSIVE_JSON_FACTORY.createParser(reader);
      PERMISSIVE_OBJECT_MAPPER.readTree(parser); //consume everything avro would
      endOfSchemaLocation = parser.getCurrentLocation();
      int charOffset = (int) endOfSchemaLocation.getCharOffset();
      if (charOffset >= json.length()) {
        return;
      }
      dangling = json.substring(charOffset).trim();
    } catch (Exception e) {
      throw new IllegalStateException("error parsing json out of " + json, e);
    }
    if (!dangling.isEmpty()) {
      throw new IllegalArgumentException("dangling content beyond the end of a schema at line: "
          + endOfSchemaLocation.getLineNr() + " column: " + endOfSchemaLocation.getColumnNr() + ": " + dangling);
    }
  }

  private static boolean isAMathematicalInteger(BigDecimal bigDecimal) {
    return bigDecimal.stripTrailingZeros().scale() <= 0;
  }
}
