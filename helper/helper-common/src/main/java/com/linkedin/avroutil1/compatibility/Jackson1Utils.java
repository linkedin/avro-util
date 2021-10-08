/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;


public class Jackson1Utils {
  private static final String BYTES_CHARSET = "ISO-8859-1";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

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
}
