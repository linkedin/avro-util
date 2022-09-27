/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.exception;

public class AvroUtilException extends Exception {

  public AvroUtilException(Exception e) {
    super(e);
  }
  public AvroUtilException(String message) {
    super(message);
  }
}
