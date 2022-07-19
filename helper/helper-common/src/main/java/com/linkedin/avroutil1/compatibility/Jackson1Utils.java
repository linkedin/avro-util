/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonLocation;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.LongNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.avro.Schema.Type.UNION;


public class Jackson1Utils {
  private static final String BYTES_CHARSET = "ISO-8859-1";
  private static final JsonFactory JSON_FACTORY;
  private static final JsonFactory PERMISSIVE_JSON_FACTORY;
  private static final ObjectMapper OBJECT_MAPPER;
  private static final ObjectMapper PERMISSIVE_OBJECT_MAPPER;

  private static final Logger LOGGER = LoggerFactory.getLogger(Jackson1Utils.class);

  static {
    JSON_FACTORY = new JsonFactory();
    OBJECT_MAPPER = new ObjectMapper(JSON_FACTORY);
    JSON_FACTORY.setCodec(OBJECT_MAPPER);

    PERMISSIVE_JSON_FACTORY = new JsonFactory(); //match that used by vanilla avro
    PERMISSIVE_OBJECT_MAPPER = new ObjectMapper(PERMISSIVE_JSON_FACTORY);
    PERMISSIVE_JSON_FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
    PERMISSIVE_JSON_FACTORY.setCodec(PERMISSIVE_OBJECT_MAPPER);
  }

  private Jackson1Utils() {
    // Util class; should not be instantiated.
  }

  /**
   * toJsonNode takes in an arbitrary object and convert it into corresponding JsonNode
   * @param datum input datum object
   * @return generated JsonNode object.
   */
  public static JsonNode toJsonNode(Object datum) {
    if (datum == null) {
      return null;
    } else if (datum instanceof JsonNode) {
      return (JsonNode) datum;
    } else if (datum instanceof byte[]) {
      try {
        return JsonNodeFactory.instance.textNode(new String((byte[]) datum, BYTES_CHARSET));
      } catch (UnsupportedEncodingException e) {
        throw new AvroRuntimeException("unable to turn bytes into json string", e);
      }
    } else if (datum instanceof CharSequence || datum instanceof Enum<?>) {
      return JsonNodeFactory.instance.textNode(datum.toString());
    } else if (datum instanceof Double) {
      return JsonNodeFactory.instance.numberNode((Double) datum);
    } else if (datum instanceof Float) {
      return JsonNodeFactory.instance.numberNode((Float) datum);
    } else if (datum instanceof Long) {
      return JsonNodeFactory.instance.numberNode((Long) datum);
    } else if (datum instanceof Integer) {
      return JsonNodeFactory.instance.numberNode((Integer) datum);
    } else if (datum instanceof Boolean) {
      return JsonNodeFactory.instance.booleanNode((Boolean) datum);
    } else if (datum instanceof Map) {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.convertValue(datum, JsonNode.class);
    } else if (datum instanceof Collection) {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.convertValue(datum, JsonNode.class);
    } else if (datum instanceof GenericData.EnumSymbol) {
      GenericData.EnumSymbol enumSymbol = (GenericData.EnumSymbol) datum;
      return JsonNodeFactory.instance.textNode(enumSymbol.toString());
    } else if (datum instanceof GenericFixed) { //also covers specific fixed
      GenericFixed fixed = (GenericFixed) datum;
      try {
        return JsonNodeFactory.instance.textNode(new String(fixed.bytes(), BYTES_CHARSET));
      } catch (UnsupportedEncodingException e) {
        throw new AvroRuntimeException("unable to turn fixed into json string", e);
      }
    } else if (datum instanceof IndexedRecord) { //generic or specific records
      IndexedRecord record = (IndexedRecord) datum;
      ObjectNode recordNode = JsonNodeFactory.instance.objectNode();
      for (Schema.Field field : record.getSchema().getFields()) {
        Object fieldValue = record.get(field.pos());
        recordNode.put(field.name(), toJsonNode(fieldValue));
      }
      return recordNode;
    } else {
      throw new AvroRuntimeException("Unknown datum class: " + datum.getClass());
    }
  }

  // See the doc for AvroCompatibilityHelper.setFieldPropFromJsonString() for how str and strict are interpreted.
  public static JsonNode toJsonNode(String str, boolean strict) {
    if (str == null) {
      return null;
    }
    try {
      JsonParser parser = JSON_FACTORY.createJsonParser(str);
      JsonNode node = OBJECT_MAPPER.readTree(parser);
      if (node == null) {
        // Happens when str.isEmpty().
        throw new NullPointerException("str cannot be an empty string");
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
      if (bVal.isIntegralNumber() || Jackson1Utils.isRoundNumber(bVal)) {
        //we dont care about numbers larger than 64 bit
        return aVal.getLongValue() == bVal.getLongValue();
      }
      return false; //b isnt round
    } else {
      //a is float
      if (!bVal.isIntegralNumber()) {
        //a and b are floats
        //this has issues with rounding and precision, but i'd rather stick to this until someone complains
        return aVal.getDoubleValue() == bVal.getDoubleValue();
      }
      //b is integral
      return Jackson1Utils.isRoundNumber(aVal) && (aVal.getLongValue() == bVal.getLongValue());
    }
  }

  public static boolean isRoundNumber(JsonNode node) {
    if (node == null || !node.isNumber()) {
      return false;
    }
    if (node.isIntegralNumber()) {
      return true;
    }
    double actual = node.getDoubleValue();
    return Double.compare(Math.floor(actual), Math.ceil(actual)) == 0;
  }

  /**
   *  Enforces uniform numeric default values across Avro versions
   */
  public static JsonNode enforceUniformNumericDefaultValues(Schema.Field field) {
    JsonNode defaultValue = field.defaultValue();
    BigDecimal numericValue = defaultValue.getDecimalValue();
    Schema schema = field.schema();
    // a default value for a union, must match the first element of the union
    Schema.Type defaultType = schema.getType() == UNION ? schema.getTypes().get(0).getType() : schema.getType();

    switch (defaultType) {
      case INT:
        if (!isAMathematicalInteger(numericValue)) {
          LOGGER.warn(String.format("Invalid default value: %s for \"int\" field: %s", field.defaultValue(), field.name()));
          return defaultValue;
        }
        return new IntNode(defaultValue.getNumberValue().intValue());
      case LONG:
        if (!isAMathematicalInteger(numericValue)) {
          LOGGER.warn(String.format("Invalid default value: %s for \"long\" field: %s", field.defaultValue(), field.name()));
          return defaultValue;
        }
        return new LongNode(defaultValue.getNumberValue().longValue());
      case DOUBLE:
        return new DoubleNode(defaultValue.getNumberValue().doubleValue());
      case FLOAT:
        return new DoubleNode(defaultValue.getNumberValue().floatValue());
      default:
        return defaultValue;
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
      JsonParser parser = PERMISSIVE_JSON_FACTORY.createJsonParser(reader);
      PERMISSIVE_OBJECT_MAPPER.readTree(parser); //consume everything avro would
      endOfSchemaLocation = parser.getCurrentLocation();
      int charOffset = (int) endOfSchemaLocation.getCharOffset();
      if (charOffset >= json.length() - 1) {
        return;
      }
      dangling = json.substring(charOffset + 1).trim();
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
