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

  /**
   * operate on the schema normalizing any json properties as needed
   * @param schema
   * @param gen
   * @return
   */
  public void execute(Schema schema, G gen) {
  }

  /**
   * operate on the field normalizing any json properties as needed
   * @param field
   * @param gen
   * @return
   */
  public void execute(Schema.Field field, G gen) {
  }

  public String getPropName() {
    return PROP_NAME;
  }
}
