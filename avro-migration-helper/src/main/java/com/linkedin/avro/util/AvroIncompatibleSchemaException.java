/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.util;

public class AvroIncompatibleSchemaException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public AvroIncompatibleSchemaException(Throwable t) {
    super(t);
  }

  public AvroIncompatibleSchemaException(String message, Throwable cause) {
    super(message, cause);
  }

  public AvroIncompatibleSchemaException(String msg) {
    super(msg);
  }
}