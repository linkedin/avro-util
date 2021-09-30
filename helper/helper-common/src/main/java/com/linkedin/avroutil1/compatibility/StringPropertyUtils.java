/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.TextNode;


// Helper methods for Avro versions that only support string props, i.e., Avro <= 1.7.2.
public class StringPropertyUtils {

  private StringPropertyUtils() {
    // Util class; should not be instantiated.
  }

  public static String getFieldPropAsJsonString(Schema.Field field, String name) {
    String val = field.getProp(name);
    return val == null ? null : Jackson1Utils.toJsonString(new TextNode(val));
  }

  public static void setFieldPropFromJsonString(Schema.Field field, String name, String value, boolean strict) {
    if (StringUtils.isQuoted(value)) {
      JsonNode node = Jackson1Utils.toJsonNode(value, strict);
      value = node.getTextValue();
    } else if (strict) {
      throw new IllegalArgumentException(
          "value " + value + " must be properly quoted before it can be set for property " + name + " in schema "
              + field.name() + "; this Avro supports only string props");
    }
    field.addProp(name, value);
  }

  public static String getSchemaPropAsJsonString(Schema schema, String name) {
    String val = schema.getProp(name);
    return val == null ? null : Jackson1Utils.toJsonString(new TextNode(val));
  }

  public static void setSchemaPropFromJsonString(Schema schema, String name, String value, boolean strict) {
    if (StringUtils.isQuoted(value)) {
      JsonNode node = Jackson1Utils.toJsonNode(value, strict);
      value = node.getTextValue();
    } else if (strict) {
      throw new IllegalArgumentException(
          "value " + value + " must be properly quoted before it can be set for property " + name + " in schema "
              + schema.getName() + "; this Avro supports only string props");
    }
    schema.addProp(name, value);
  }
}
