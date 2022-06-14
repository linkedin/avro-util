/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.vanilla;

public interface SchemaSetProvider {

  @Deprecated //for backwards compatibility
  default SchemaSet loadSchemas() {
    return loadSchemas(true);
  }

  /**
   * loads all schemas "discoverable" by this provider
   * @param topLevelOnly true to load only top-level (outermost per source file)
   *                     false to also directly load "inner" schemas
   * @return a set of all schemas found
   */
  SchemaSet loadSchemas(boolean topLevelOnly);
}
