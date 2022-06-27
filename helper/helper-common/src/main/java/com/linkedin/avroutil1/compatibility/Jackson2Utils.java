/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.avro.Schema.Type.UNION;


public class Jackson2Utils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  private static final Logger LOGGER = LoggerFactory.getLogger(Jackson2Utils.class);

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
    BigDecimal numericDefaultValue = genericDefaultValue.decimalValue();
    Schema schema = field.schema();
    // a default value for a union, must match the first element of the union
    Schema.Type defaultType = schema.getType() == UNION ? schema.getTypes().get(0).getType() : schema.getType();

    switch (defaultType) {
      case INT:
        if (!isAMathematicalInteger(numericDefaultValue)) {
          LOGGER.warn(String.format("Invalid default value: %s for \"int\" field: %s", genericDefaultValue, field.name()));
          return genericDefaultValue;
        }
        return new IntNode(genericDefaultValue.intValue());
      case LONG:
        if (!isAMathematicalInteger(numericDefaultValue)) {
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

  private static boolean isAMathematicalInteger(BigDecimal bigDecimal) {
    return bigDecimal.stripTrailingZeros().scale() <= 0;
  }
}
