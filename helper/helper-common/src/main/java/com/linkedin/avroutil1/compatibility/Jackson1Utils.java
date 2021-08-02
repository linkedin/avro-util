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
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;


public class Jackson1Utils {
  static final String BYTES_CHARSET = "ISO-8859-1";

  /**
   * toJsonNode takes in an arbitrary object and convert it into corresponding JsonNode
   * @param datum input datum object
   * @return generated JsonNode object.
   */
  public static JsonNode toJsonNode(Object datum) {
    if (datum == null) {
      return JsonNodeFactory.instance.nullNode();
    } else if (datum instanceof byte[]) {
      try {
        return JsonNodeFactory.instance.textNode(new String((byte[]) datum, BYTES_CHARSET));
      } catch (UnsupportedEncodingException e) {
        throw new AvroRuntimeException(e);
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
    } else {
      throw new AvroRuntimeException("Unknown datum class: " + datum.getClass());
    }
  }

}
