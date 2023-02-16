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
public abstract class AvscWriterPlugin<G extends JsonGeneratorWrapper<?>> {

  protected final String PROP_NAME;

  protected AvscWriterPlugin(String prop_name) {
    PROP_NAME = prop_name;
  }

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
  public String execute(Schema schema, G gen) {
    return null;
  }

  /**
   * returns the handled JSON property name for Schema.Field
   * @param field
   * @param gen
   * @return
   */
  public String execute(Schema.Field field, G gen) {
    return null;
  }

  public String getPropName() {
    return PROP_NAME;
  }
}
