/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.avro.Schema;


/**
 * a more configurable alternative to {@link org.apache.avro.Schema#equals(Object)}
 */
public class ConfigurableSchemaComparator {

  public static boolean equals(Schema a, Schema b, SchemaComparisonConfiguration config) {
    validateConfig(config);
    Set<SeenPair> seen = new HashSet<>(3);
    return equals(a, b, config, seen);
  }

  private static void validateConfig(SchemaComparisonConfiguration config) {
    if (config == null) {
      throw new IllegalArgumentException("config required");
    }
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_7) && config.isCompareNonStringJsonProps()) {
      //1.7 itself changes between < 1.7.3 and >= 1.7.3, so we leave that validation to later runtime :-(
      throw new IllegalArgumentException(
          "avro " + runtimeAvroVersion + " does not preserve non-string props and so cannot compare them");
    }
  }

  private static boolean equals(Schema a, Schema b, SchemaComparisonConfiguration config, Set<SeenPair> seen) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    Schema.Type type = a.getType();
    if (!Objects.equals(type, b.getType())) {
      return false;
    }

    boolean considerJsonStringProps = config.isCompareStringJsonProps();
    boolean considerJsonNonStringProps = config.isCompareNonStringJsonProps();
    boolean considerAliases = config.isCompareAliases();
    boolean considerJsonProps = considerJsonStringProps || considerJsonNonStringProps;
    boolean considerDocs = config.isCompareFieldDocs();
    Set<String> jsonPropNamesToIgnore = config.getJsonPropNamesToIgnore();

    if (considerJsonProps && !hasSameObjectProps(a, b, considerJsonStringProps, considerJsonNonStringProps,
        jsonPropNamesToIgnore)) {
      return false;
    }

    if (considerDocs && !Objects.equals(a.getDoc(), b.getDoc())) {
      return false;
    }

    switch (type) {
      //all of these have nothing more to compare by beyond their type and props
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
        return true;

      //named types

      case ENUM:
        return a.getFullName().equals(b.getFullName()) && (!considerAliases || hasSameAliases(a, b))
            //list comparison is sensitive to order
            && a.getEnumSymbols().equals(b.getEnumSymbols());
      case FIXED:
        return a.getFullName().equals(b.getFullName()) && (!considerAliases || hasSameAliases(a, b))
            && a.getFixedSize() == b.getFixedSize();
      case RECORD:
        return recordSchemaEquals(a, b, config, seen);

      //collections and union

      case ARRAY:
        return equals(a.getElementType(), b.getElementType(), config, seen);
      case MAP:
        return equals(a.getValueType(), b.getValueType(), config, seen);
      case UNION:
        List<Schema> aBranches = a.getTypes();
        List<Schema> bBranches = b.getTypes();
        if (aBranches.size() != bBranches.size()) {
          return false;
        }
        for (int i = 0; i < aBranches.size(); i++) {
          Schema aBranch = aBranches.get(i);
          Schema bBranch = bBranches.get(i);
          if (!equals(aBranch, bBranch, config, seen)) {
            return false;
          }
        }
        return true;
      default:
        throw new IllegalStateException("unhandled: " + type);
    }
  }

  private static boolean recordSchemaEquals(Schema a, Schema b, SchemaComparisonConfiguration config,
      Set<SeenPair> seen) {
    if (!a.getFullName().equals(b.getFullName())) {
      return false;
    }
    //loop protection for self-referencing schemas
    SeenPair pair = new SeenPair(a, b);
    if (seen.contains(pair)) {
      return true;
    }
    seen.add(pair);

    boolean considerJsonStringProps = config.isCompareStringJsonProps();
    boolean considerJsonNonStringProps = config.isCompareNonStringJsonProps();
    boolean considerAliases = config.isCompareAliases();
    boolean considerJsonProps = considerJsonStringProps || considerJsonNonStringProps;
    boolean compareIntToFloatDefaults = config.isCompareIntToFloatDefaults();
    boolean compareDocs = config.isCompareFieldDocs();
    Set<String> jsonPropNamesToIgnore = config.getJsonPropNamesToIgnore();

    try {
      if (considerAliases && !hasSameAliases(a, b)) {
        return false;
      }

      if (considerJsonProps && !hasSameObjectProps(a, b, considerJsonStringProps, considerJsonNonStringProps,
          jsonPropNamesToIgnore)) {
        return false;
      }

      List<Schema.Field> aFields = a.getFields();
      List<Schema.Field> bFields = b.getFields();
      if (aFields.size() != bFields.size()) {
        return false;
      }
      for (int i = 0; i < aFields.size(); i++) {
        Schema.Field aField = aFields.get(i);
        Schema.Field bField = bFields.get(i);

        if (!aField.name().equals(bField.name())) {
          return false;
        }
        if (!equals(aField.schema(), bField.schema(), config, seen)) {
          return false;
        }
        if (AvroCompatibilityHelper.fieldHasDefault(aField) && AvroCompatibilityHelper.fieldHasDefault(bField)) {
          boolean defaultsEqual = AvroCompatibilityHelper.defaultValuesEqual(aField, bField, compareIntToFloatDefaults);
          if (!defaultsEqual) {
            return false;
          }
        } else if (AvroCompatibilityHelper.fieldHasDefault(aField) || AvroCompatibilityHelper.fieldHasDefault(bField)) {
          //means one field has a default value and the other does not
          return false;
        }
        if (!Objects.equals(aField.order(), bField.order())) {
          return false;
        }
        if (considerJsonProps && !hasSameObjectProps(aField, bField, considerJsonStringProps,
            considerJsonNonStringProps, jsonPropNamesToIgnore)) {
          return false;
        }
        if (considerAliases && !hasSameAliases(aField, bField)) {
          return false;
        }
        if (compareDocs && !aField.doc().equals(bField.doc())) {
          return false;
        }
      }
      return true;
    } finally {
      seen.remove(pair);
    }
  }

  private static boolean hasSameAliases(Schema a, Schema b) {
    return Objects.equals(a.getAliases(), b.getAliases());
  }

  private static boolean hasSameAliases(Schema.Field a, Schema.Field b) {
    Set<String> aAliases = AvroCompatibilityHelper.getFieldAliases(a);
    Set<String> bAliases = AvroCompatibilityHelper.getFieldAliases(b);
    return Objects.equals(aAliases, bAliases);
  }

  private static boolean hasSameObjectProps(Schema a, Schema b, boolean compareStringProps,
      boolean compareNonStringProps, Set<String> jsonPropNamesToIgnore) {

    return AvroCompatibilityHelper.sameJsonProperties(a, b, compareStringProps, compareNonStringProps,
        jsonPropNamesToIgnore);
  }

  private static boolean hasSameObjectProps(Schema.Field a, Schema.Field b, boolean compareStringProps,
      boolean compareNonStringProps, Set<String> jsonPropNamesToIgnore) {
    return AvroCompatibilityHelper.sameJsonProperties(a, b, compareStringProps, compareNonStringProps,
        jsonPropNamesToIgnore);
  }

  private static class SeenPair {
    private final Schema s1;
    private final Schema s2;

    public SeenPair(Schema s1, Schema s2) {
      this.s1 = s1;
      this.s2 = s2;
    }

    public boolean equals(Object o) {
      if (!(o instanceof SeenPair)) {
        return false;
      }
      return this.s1 == ((SeenPair) o).s1 && this.s2 == ((SeenPair) o).s2;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(s1) + System.identityHashCode(s2);
    }
  }
}
