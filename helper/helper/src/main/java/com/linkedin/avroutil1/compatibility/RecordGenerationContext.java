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

  private final int MAX_DEPTH = 100;
  private boolean hasHitMaxDepth = false;

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

  public boolean hasHitMaxDepth() {
    if (hasHitMaxDepth) {
      return true;
    }

    if (path.size() > MAX_DEPTH) {
      this.hasHitMaxDepth = true;
    }
    return hasHitMaxDepth;
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
