/*
 * Copyright 2025 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.util;

import com.linkedin.avroutil1.compatibility.ConfigurableSchemaComparator;
import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.model.AvroSchema;
import java.util.Set;
import org.apache.avro.Schema;

/**
 * Centralized helpers for schema equality checks used across builder flows.
 */
public final class SchemaComparisonUtil {

  private SchemaComparisonUtil() { }

  public static SchemaComparisonConfiguration buildConfig(Set<String> jsonPropsToIgnore) {
    SchemaComparisonConfiguration config = SchemaComparisonConfiguration.STRICT;
    if (jsonPropsToIgnore != null && !jsonPropsToIgnore.isEmpty()) {
      config = config.jsonPropNamesToIgnore(jsonPropsToIgnore);
    }
    return config;
  }

  public static boolean equalsApacheSchema(Schema a, Schema b, Set<String> jsonPropsToIgnore) {
    if (a == null || b == null) {
      return a == b;
    }
    return ConfigurableSchemaComparator.equals(a, b, buildConfig(jsonPropsToIgnore));
  }

  public static boolean equalsAvroSchema(AvroSchema a, AvroSchema b, Set<String> jsonPropsToIgnore) {
    if (a == null || b == null) {
      return a == b;
    }
    return com.linkedin.avroutil1.util.ConfigurableAvroSchemaComparator.equals(a, b, buildConfig(jsonPropsToIgnore));
  }
}
