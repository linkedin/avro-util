/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final Map<String, Integer> pathOccurrences = new HashMap<>();

  public RecordGenerationConfig getConfig() {
    return config;
  }

  public void pushPath(Schema path) {
    if (path == null) {
      throw new IllegalArgumentException("path required");
    }
    this.path.add(path);
    if (path.getType().equals(Schema.Type.RECORD)) {
      String pathName = path.getFullName();
      pathOccurrences.put(pathName, pathOccurrences.getOrDefault(pathName, 0) + 1);
    }
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
    return pathOccurrences.getOrDefault(currentSchemaName, 0) >= MAX_DEPTH;
  }

  public Schema popPath() {
    if (path.isEmpty()) {
      throw new IllegalStateException("this is a bug");
    }
    if (path.get(path.size() - 1).getType() == Schema.Type.RECORD) {
      String pathName = path.get(path.size() - 1).getFullName();
      pathOccurrences.computeIfPresent(pathName, (k, v) -> v - 1);
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
