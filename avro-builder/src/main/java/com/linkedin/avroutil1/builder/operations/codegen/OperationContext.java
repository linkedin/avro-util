/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen;

import com.linkedin.avroutil1.builder.operations.AvroSchemaUtils;
import com.linkedin.avroutil1.model.AvroSchema;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import org.apache.avro.Schema;


/**
 * common context to all {@link com.linkedin.avroutil1.builder.operations.Operation}s run during an execution
 */
public class OperationContext {
  private final HashMap<String, Object> context;
  private Set<File> avroFiles;
  private Set<AvroSchema> avroSchemas;

  public OperationContext() {
    this.context = new HashMap<>();
  }

  public void addParsedSchemas(Set<AvroSchema> avroSchemas, Set<File> avroFiles) {
    if (this.avroFiles != null || this.avroSchemas != null) {
      throw new IllegalStateException("Cannot initialize avro files twice");
    }

    this.avroFiles = Collections.unmodifiableSet(avroFiles);
    this.avroSchemas = Collections.unmodifiableSet(avroSchemas);
  }

  public void addVanillaSchemas(Set<Schema> schemas, Set<File> avroFiles) {
    addParsedSchemas(AvroSchemaUtils.schemasToAvroSchemas(schemas), avroFiles);
  }

  /**
   * Returns an unmodifiable set of all input avro files.
   */
  public Set<File> getAvroFiles() {
    if (this.avroFiles == null) {
      throw new IllegalStateException("Avro files haven't been added yet.");
    }
    return this.avroFiles;
  }

  /**
   * Returns an unmodifiable set of all parsed avro schemas.
   */
  public Set<AvroSchema> getAvroSchemas() {
    if (this.avroSchemas == null) {
      throw new IllegalStateException("Avro schemas haven't been added yet.");
    }

    return this.avroSchemas;
  }

  public void setConfigValue(String key, Object value) {
    if (this.context.containsKey(key)) {
      throw new IllegalStateException("Cannot initialize an already-initialized config key.");
    }
    this.context.put(key, value);
  }

  public Object getConfigValue(String key) {
    if (!this.context.containsKey(key)) {
      throw new IllegalStateException("No value for key " + key);
    }
    return this.context.get(key);
  }
}
