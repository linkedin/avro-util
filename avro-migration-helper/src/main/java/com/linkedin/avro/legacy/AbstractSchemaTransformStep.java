/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

//package-private ON PURPOSE
abstract class AbstractSchemaTransformStep implements SchemaTransformStep {
  protected final Exception _cause;

  AbstractSchemaTransformStep(Exception cause) {
    _cause = cause;
  }
}
