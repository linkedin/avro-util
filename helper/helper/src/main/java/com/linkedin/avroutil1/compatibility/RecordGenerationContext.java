/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;


/**
 * state for a single generation operation
 */
public class RecordGenerationContext {
  private final RecordGenerationConfig config;
  private final int MAX_DEPTH = 3;

  public RecordGenerationContext(RecordGenerationConfig config) {
    this.config = config;
  }

  //TODO - replace with JsonPath in the future if that ever happens
  private final List<Schema> path = new ArrayList<>();

  public RecordGenerationConfig getConfig() {
    return config;
  }

  public void pushPath(Schema path) {
    if (path == null) {
      throw new IllegalArgumentException("path required");
    }
    this.path.add(path);
  }

  /**
   * Checks if the current schema has been recursively-generated beyond the max recursive depth.
   * Only applies to records since only records can be recursive.
   */
  public boolean hasHitMaxRecursiveDepth() {
    Schema currSchema = path.get(path.size() - 1);
    if (currSchema.getType() != Schema.Type.RECORD) {
      return false;
    }

    String currentSchemaName = path.get(path.size() - 1).getFullName();
    int numberOfOccurrences = 0;
    for (Schema schema : path) {
      if (schema.getType() == Schema.Type.RECORD && schema.getFullName().equals(currentSchemaName)) {
        numberOfOccurrences++;
      }
      if (numberOfOccurrences >= MAX_DEPTH) {
        return true;
      }
    }
    return false;
  }

  public Schema popPath() {
    if (path.isEmpty()) {
      throw new IllegalStateException("this is a bug");
    }
    return path.remove(path.size() - 1);
  }

  public int seen(Schema schema) {
    int count = 0;
    for (Schema element : path) {
      if (element == schema) {
        count++;
      }
    }
    return count;
  }
}
