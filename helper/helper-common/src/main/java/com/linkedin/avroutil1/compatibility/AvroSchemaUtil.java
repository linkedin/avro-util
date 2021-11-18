/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;


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
      throw new IllegalArgumentException("schema must not be null");
    }
    if (schema.getType() != Schema.Type.UNION) {
      return schema;  // schema is not a union.
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

  /**
   * we want to be very generous with what we let users provide for default values.
   * sadly, (modern) avro can only handle specific classes/collections/primitive-wrappers
   * (see org.apache.avro.util.internal.JacksonUtils.toJson(Object, JsonGenerator) in 1.9+)
   * @param mightNotBeFriendly a proposed field default value that might originate from
   *                           a call like AvroCompatibilityHelper.getGenericDefaultValue()
   * @return a representation of the input that avro likes for use as a field default value
   */
  public static Object avroFriendlyDefaultValue(Object mightNotBeFriendly) throws Exception {

    //generic enums we turn to strings
    if (mightNotBeFriendly instanceof GenericData.EnumSymbol) {
      return mightNotBeFriendly.toString(); // == symbol string
    }

    //fixed (generic or specific) we turn to bytes
    if (mightNotBeFriendly instanceof GenericFixed) {
      return ((GenericFixed) mightNotBeFriendly).bytes();
    }

    //records (generic or specific) we turn to maps
    if (mightNotBeFriendly instanceof IndexedRecord) {
      IndexedRecord record = (IndexedRecord) mightNotBeFriendly;
      Schema recordSchema = record.getSchema();

      Map<String, Object> map = new HashMap<>();
      for (Schema.Field field : recordSchema.getFields()) {
        Object fieldValue = record.get(field.pos());
        map.put(field.name(), avroFriendlyDefaultValue(fieldValue));
        //TODO - extra props ?!
      }
      return map;
    }
    return mightNotBeFriendly;
  }

  /**
   * tests if a schema is impacted by avro-702 (the avsc result of Schema.toString() does
   * not match the input schema).
   * this can only be done under avro 1.4, but for dependency reasons we cant assert on runtime avro
   * @param schema a schema to test
   * @return true if schema is impacted by avro-702
   */
  public static boolean isImpactedByAvro702(Schema schema) {
    String naiveAvsc = schema.toString(true);
    boolean parseFailed = false;
    Schema evilTwin = null;
    try {
      evilTwin = Schema.parse(naiveAvsc); //avro-702 can result in "exploded" schemas that dont parse
    } catch (Exception ignored) {
      parseFailed = true;
    }
    return parseFailed || !evilTwin.equals(schema);
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
