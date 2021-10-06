/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

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
   * Returns true if a null value is allowed as the default value for a field
   * (given its schema). It is valid if and only if:
   * (1) The field's type is null, or
   * (2) The field is a union, where the first alternative type is null.
   */
  public static boolean isNullAValidDefaultForSchema(Schema schema) {
    return schema != null &&
           (schema.getType() == Schema.Type.NULL ||
            schema.getType() == Schema.Type.UNION &&
            !schema.getTypes().isEmpty() &&
            schema.getTypes().get(0).getType() == Schema.Type.NULL);
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
    return findNonNullUnionBranch(field.schema());
  }

  /**
   * Given a union schema with exactly one non-null branch, return that non-null branch.
   * If the schema is not a union, return it as is.
   * @param schema a union schema containing exactly one non-null branch, or a non-union schema.
   * @return the non-null union branch, or the original schema.
   */
  public static Schema findNonNullUnionBranch(Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("scheme must not be null");
    }
    if (schema.getType() != Schema.Type.UNION) {
      return schema;  // field is not a union.
    }
    List<Schema> branches = schema.getTypes();
    List<Schema> nonNullBranches = branches.stream().
        filter(branch -> branch.getType() != Schema.Type.NULL).collect(Collectors.toList());
    if (nonNullBranches.size() != 1) {
      throw new IllegalArgumentException(String.format("schema has %d non-null union branches, where exactly 1 is expected",
          nonNullBranches.size()));
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
          visitor.visitField(schema, field);
          traverseSchema(field.schema(), visitor, visited);
        }
        break;
      default:
    }
  }
}
