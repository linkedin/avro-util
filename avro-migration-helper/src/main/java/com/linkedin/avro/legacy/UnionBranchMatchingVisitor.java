/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import com.linkedin.avro.util.SchemaVisitor;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.avro.Schema;


/**
 * a schema visitor that matches union branch schemas
 * and hands matched schemas to a provided consumer
 */
class UnionBranchMatchingVisitor implements SchemaVisitor {

  private Function<Schema, Boolean> matcher;
  private Consumer<Schema> matchConsumer;

  public UnionBranchMatchingVisitor(Function<Schema, Boolean> matcher, Consumer<Schema> matchConsumer) {
    this.matcher = matcher;
    this.matchConsumer = matchConsumer;
  }

  @Override
  public void visitField(Schema.Field field) {
    Schema fieldSchema = field.schema();
    if (fieldSchema.getType() != Schema.Type.UNION) {
      return;
    }
    Schema found = null;
    for (Schema possibleType : fieldSchema.getTypes()) {
      if (matcher.apply(possibleType)) {
        if (found != null) {
          throw new IllegalArgumentException(
              String.format("multiple matches in a single union: %s and %s", found.getFullName(), possibleType.getFullName())
          );
        }
        found = possibleType;
      }
    }
    if (found != null) {
      matchConsumer.accept(found);
    }
  }
}
