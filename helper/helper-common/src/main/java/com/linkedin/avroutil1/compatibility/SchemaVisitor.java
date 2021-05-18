/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;


public interface SchemaVisitor {

  default void visitSchema(Schema schema) {

  }

  default void visitField(Schema parent, Schema.Field field) {

  }
}
