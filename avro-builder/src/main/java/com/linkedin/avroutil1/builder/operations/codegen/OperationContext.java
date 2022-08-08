/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen;

import java.io.File;
import java.util.HashMap;
import java.util.Set;


/**
 * common context to all {@link com.linkedin.avroutil1.builder.operations.Operation}s run during an execution
 */
public class OperationContext {
  private Set<File> avroFiles;
  private final HashMap<String, Object> context;

  public OperationContext() {
    this.context = new HashMap<>();
  }

  public void setAvroFiles(Set<File> avroFiles) {
    if (this.avroFiles != null) {
      throw new IllegalStateException("Cannot initialize avro files twice");
    }
    this.avroFiles = avroFiles;
  }

  public Set<File> getAvroFiles() {
    if (avroFiles == null) {
      throw new IllegalStateException("Avro files are undefined");
    }

    return this.avroFiles;
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
