/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import org.apache.avro.Schema;


public interface SchemaVisitor {
  default void visitSchema(Schema schema) {

  }

  default void visitField(Schema.Field field) {

  }
}