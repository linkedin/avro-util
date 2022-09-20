/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field.Order;


/**
 * an builder interface to build a Schema.Field with given parameters.
 * The defaultValue needed to be defined in implemented classes since default
 * value type is changed to Object since AVRO 1.9
 */
public interface FieldBuilder {

  FieldBuilder setName(String name);

  FieldBuilder setSchema(Schema schema);

  FieldBuilder setDoc(String doc);

  FieldBuilder setDefault(Object defaultValue);

  FieldBuilder setOrder(Order order);

  /**
   * @param jsonLiteral a JSON object serialized in String form.
   * For example, a JSON object, {"key" : 123}, should be serialized as the following string: "{\"key\":123}".
   */
  FieldBuilder addProp(String propName, String jsonLiteral);

  /**
   * @param propNameToJsonObjectMap the key is the propName and the value is a JSON object serialized in String form.
   * Please see {@link #addProp(String, String)} for more details on the JSON serialized String.
   */
  FieldBuilder addProps(Map<String, String> propNameToJsonObjectMap);

  FieldBuilder removeProp(String propName);

  @Deprecated
  FieldBuilder copyFromField();

  Schema.Field build();
}
