/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.exception;

import java.util.ArrayList;
import java.util.List;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;


public class AvroUtilMissingFieldException extends AvroUtilException{
  private List<Schema.Field> chainOfFields = new ArrayList<>(8);

  public AvroUtilMissingFieldException(AvroRuntimeException e) {
    super(e);
  }

  public AvroUtilMissingFieldException(String message, Schema.Field field) {
    super(message);
    chainOfFields.add(field);
  }

  public void addParentField(Schema.Field field) {
    chainOfFields.add(field);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (Schema.Field field : chainOfFields) {
      result.insert(0, " --> " + field.name());
    }
    return "Path in schema:" + result;
  }
}
