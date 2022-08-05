/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroLiteral;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.CodeLocation;
import com.linkedin.avroutil1.parser.jsonpext.JsonValueExt;


/**
 * "placeholder" literal containing raw unparsed json
 */
public class AvscUnparsedLiteral extends AvroLiteral {
  private final JsonValueExt defaultValueNode;

  public AvscUnparsedLiteral(JsonValueExt defaultValueNode, CodeLocation codeLocation) {
    super(codeLocation);
    this.defaultValueNode = defaultValueNode;
  }

  @Override
  public AvroSchema getSchema() {
    throw new IllegalStateException("should not be called on a " + getClass().getName());
  }

  @Override
  public AvroType type() {
    throw new IllegalStateException("should not be called on a " + getClass().getName());
  }

  public JsonValueExt getDefaultValueNode() {
    return defaultValueNode;
  }
}
