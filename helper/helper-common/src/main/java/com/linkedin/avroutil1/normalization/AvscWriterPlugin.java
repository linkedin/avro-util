/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.normalization;

import com.linkedin.avroutil1.compatibility.JsonGeneratorWrapper;
import org.apache.avro.Schema;


/***
 * AvscWriter is written to write AVSC json from avro Schemas.
 * This plugin allows you to define functionality to handle an extra JSON property differently
 */
public interface AvscWriterPlugin<G extends JsonGeneratorWrapper<?>> {
  enum AvscWriterPluginLevel {
    SCHEMA,
    FIELD
  }

  /**
   * returns the handled JSON property name
   * @param schema
   * @param gen
   * @return
   */
  default String execute(Schema schema, G gen) {
    return null;
  }

  /**
   * returns the handled JSON property name for Schema.Field
   * @param field
   * @param gen
   * @return
   */
  default String execute(Schema.Field field, G gen) {
    return null;
  }
}
