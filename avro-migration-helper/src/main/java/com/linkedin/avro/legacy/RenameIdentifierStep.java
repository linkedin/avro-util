/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import java.util.regex.Pattern;


//package private ON PURPOSE
class RenameIdentifierStep extends AbstractSchemaTransformStep {
  private final String _from;
  private final String _to;

  RenameIdentifierStep(Exception cause, String from, String to) {
    super(cause);
    if (from == null || from.trim().isEmpty()) {
      throw new IllegalArgumentException(String.format("illegal value for from parameter \"%s\"", from));
    }
    if (to == null || to.trim().isEmpty()) {
      throw new IllegalArgumentException(String.format("illegal value for to parameter \"%s\"", to));
    }
    _from = from;
    _to = to;
  }

  String getFrom() {
    return _from;
  }

  String getTo() {
    return _to;
  }

  @Override
  public String applyToText(String text) {
    return text.replaceAll(Pattern.quote(_from), _to);
  }

  @Override
  public SchemaTransformStep inverse() {
    return new RenameIdentifierStep(_cause, _to, _from);
  }

  @Override
  public String toString() {
    return _from + " --> " + _to;
  }
}
