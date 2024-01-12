/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations;

import com.linkedin.avroutil1.model.AvroSchema;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;
import java.util.Set;


/**
 * common context to all {@link com.linkedin.avroutil1.builder.operations.Operation}s run during an execution
 */
public class OperationContext {
  private final HashMap<String, Object> context;
  private final Set<File> avroFiles;
  private final Set<AvroSchema> avroSchemas;
  private final SchemaSet lookupSchemaSet;

  public OperationContext(Set<AvroSchema> avroSchemas, Set<File> avroFiles, SchemaSet lookupSchemaSet) {
    this.context = new HashMap<>();
    this.avroSchemas = Collections.unmodifiableSet(Objects.requireNonNull(avroSchemas));
    this.avroFiles = Collections.unmodifiableSet(Objects.requireNonNull(avroFiles));
    this.lookupSchemaSet = lookupSchemaSet;
  }

  /**
   * Returns an unmodifiable set of all input avro files.
   */
  public Set<File> getAvroFiles() {
    return this.avroFiles;
  }

  /**
   * Returns an unmodifiable set of all parsed avro schemas.
   */
  public Set<AvroSchema> getAvroSchemas() {
    return this.avroSchemas;
  }

  /**
   * Returns the lookup schema set.
   */
  public SchemaSet getLookupSchemaSet() {
    return this.lookupSchemaSet;
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
