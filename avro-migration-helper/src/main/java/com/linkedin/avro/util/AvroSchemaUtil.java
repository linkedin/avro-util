/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.util;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


public class AvroSchemaUtil {
  private AvroSchemaUtil() {
    //util class
  }

  public static void traverseSchema(Schema schema, SchemaVisitor visitor) {
    IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
    traverseSchema(schema, visitor, visited);
  }

  /**
   * given a (parent) schema, and a field name, find the schema for that field.
   * if the field is a union, returns the (only) non-null branch of the union
   * @param parent parent schema containing field
   * @param fieldName name of the field in question
   * @return schema of the field (or non-null union branch thereof)
   */
  public static Schema findNonNullUnionBranch(Schema parent, String fieldName) {
    if (parent == null || fieldName == null || fieldName.isEmpty()) {
      throw new IllegalArgumentException("arguments must not be null/empty");
    }
    Schema.Field field = parent.getField(fieldName);
    if (field == null) {
      return null;
    }
    Schema fieldSchema = field.schema();
    Schema.Type fieldSchemaType = fieldSchema.getType();
    if (!Schema.Type.UNION.equals(fieldSchemaType)) {
      return fieldSchema; //field is not a union
    }
    List<Schema> branches = fieldSchema.getTypes();
    List<Schema> nonNullBranches = branches.stream().
      filter(schema -> !Schema.Type.NULL.equals(schema.getType())).collect(Collectors.toList());
    if (nonNullBranches.size() != 1) {
      throw new IllegalArgumentException(String.format("field %s has %d non-null union branches, where exactly 1 is expected in %s",
        fieldName, nonNullBranches.size(), parent));
    }
    return nonNullBranches.get(0);
  }

  private static void traverseSchema(Schema schema, SchemaVisitor visitor, IdentityHashMap<Object, Boolean> visited) {
    if (visited.put(schema, Boolean.TRUE) != null) {
      return; //been there, done that
    }
    visitor.visitSchema(schema);
    switch (schema.getType()) {
      case UNION:
        for (Schema unionBranch : schema.getTypes()) {
          traverseSchema(unionBranch, visitor, visited);
        }
        return;
      case ARRAY:
        traverseSchema(schema.getElementType(), visitor, visited);
        return;
      case MAP:
        traverseSchema(schema.getValueType(), visitor, visited);
        return;
      case RECORD:
        for (Schema.Field field : schema.getFields()) {
          visitor.visitField(field);
          traverseSchema(field.schema(), visitor, visited);
        }
        break;
      default:
    }
  }
}
