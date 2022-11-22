/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.Jackson2Utils;
import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * a more configurable alternative to
 */
public class ConfigurableAvroSchemaComparator {

  public static boolean equals(AvroSchema a, AvroSchema b, SchemaComparisonConfiguration config) {
    validateConfig(config);
    Set<SeenPair> seen = new HashSet<>(3);
    try {
      return equals(a, b, config, seen);
    } catch (IOException e) {
      return false;
    }
  }

  private static void validateConfig(SchemaComparisonConfiguration config) {
    if (config == null) {
      throw new IllegalArgumentException("config required");
    }
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelperCommon.getRuntimeAvroVersion();
    if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_7) && config.isCompareNonStringJsonProps()) {
      //1.7 itself changes between < 1.7.3 and >= 1.7.3, so we leave that validation to later runtime :-(
      throw new IllegalArgumentException("avro " + runtimeAvroVersion + " does not preserve non-string props and so cannot compare them");
    }
  }

  private static boolean equals(AvroSchema a, AvroSchema b, SchemaComparisonConfiguration config, Set<SeenPair> seen)
      throws IOException {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    AvroType type = a.type();
    if (!Objects.equals(type, b.type())) {
      return false;
    }

    boolean considerJsonStringProps = config.isCompareStringJsonProps();
    boolean considerJsonNonStringProps = config.isCompareNonStringJsonProps();
    boolean considerAliases = config.isCompareAliases();

    switch (type) {
      //all of these have nothing more to compare by beyond their type (and we ignore props)
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
        return ((AvroEnumSchema) a).getFullName().equals(((AvroEnumSchema) b).getFullName()) && (!considerAliases || hasSameAliases((AvroEnumSchema) a, (AvroEnumSchema) b))
            //list comparison is sensitive to order
            && ((AvroEnumSchema) a).getSymbols().equals(((AvroEnumSchema) b).getSymbols());
      case FIXED:
        return ((AvroFixedSchema) a).getFullName().equals(((AvroFixedSchema) b).getFullName()) && (!considerAliases
            || hasSameAliases((AvroFixedSchema) a, (AvroFixedSchema) b))
            && ((AvroFixedSchema) a).getSize() == ((AvroFixedSchema) b).getSize();
      case RECORD:
        return recordSchemaEquals((AvroRecordSchema) a, (AvroRecordSchema) b, config, seen);

      //collections and union

      case ARRAY:
        return equals(((AvroArraySchema) a).getValueSchema(), ((AvroArraySchema) b).getValueSchema(), config, seen);
      case MAP:
        return equals(((AvroMapSchema) a).getValueSchema(), ((AvroMapSchema) b).getValueSchema(), config, seen);
      case UNION:
        List<SchemaOrRef> aBranches = ((AvroUnionSchema) a).getTypes();
        List<SchemaOrRef> bBranches = ((AvroUnionSchema) b).getTypes();
        if (aBranches.size() != bBranches.size()) {
          return false;
        }
        for (int i = 0; i < aBranches.size(); i++) {
          SchemaOrRef aBranch = aBranches.get(i);
          SchemaOrRef bBranch = bBranches.get(i);
          if (!equals(aBranch, bBranch, config, seen)) {
            return false;
          }
        }
        return true;
      default:
        throw new IllegalStateException("unhandled: " + type);
    }
  }

  private static boolean equals(SchemaOrRef aBranch, SchemaOrRef bBranch, SchemaComparisonConfiguration config, Set<SeenPair> seen)
      throws IOException {
    if (aBranch.getSchema() != null) {
      return equals(aBranch.getSchema(), bBranch.getSchema(), config, seen);
    }
    return aBranch.getRef() != null && aBranch.getRef().equals(bBranch.getRef());
  }

  private static boolean recordSchemaEquals(AvroRecordSchema a, AvroRecordSchema b, SchemaComparisonConfiguration config, Set<SeenPair> seen)
      throws IOException {
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
    boolean compareIntToFLoatDefaults = config.isCompareIntToFloatDefaults();

    try {
      if (considerAliases && !hasSameAliases(a, b)) {
        return false;
      }

      if (considerJsonProps && !hasSameObjectProps(a, b, considerJsonStringProps, considerJsonNonStringProps)) {
        return false;
      }

      List<AvroSchemaField> aFields = a.getFields();
      List<AvroSchemaField> bFields = b.getFields();
      if (aFields.size() != bFields.size()) {
        return false;
      }
      for (int i = 0; i < aFields.size(); i++) {
        AvroSchemaField aField = aFields.get(i);
        AvroSchemaField bField = bFields.get(i);

        if (!aField.getName().equals(bField.getName())) {
          return false;
        }
        if (!equals(aField.getSchema(), bField.getSchema(), config, seen)) {
          return false;
        }
        if (aField.hasDefaultValue() && bField.hasDefaultValue()) {
          if (!aField.getDefaultValue().equals(bField.getDefaultValue())) {
            return false;
          }
        } else if (aField.hasDefaultValue() || bField.hasDefaultValue()) {
          //means one field has a default value and the other does not
          return false;
        }
        if (!Objects.equals(aField.getPosition(), bField.getPosition())) {
          return false;
        }
        if (considerJsonProps && !hasSameObjectProps(aField, bField, considerJsonStringProps, considerJsonNonStringProps)) {
          return false;
        }
        if (considerAliases && !hasSameAliases(aField, bField)) {
          return false;
        }
      }
      return true;
    } finally {
      seen.remove(pair);
    }
  }

  private static boolean hasSameAliases(AvroNamedSchema a, AvroNamedSchema b) {
    return Objects.equals(a.getAliases(), b.getAliases());
  }

  private static boolean hasSameAliases(AvroSchemaField a, AvroSchemaField b) {

    Set<String> aAliases = a.aliases();
    Set<String> bAliases = b.aliases();
    return Objects.equals(aAliases, bAliases);
  }

  private static boolean hasSameObjectProps(AvroSchema a, AvroSchema b, boolean compareStringProps,
      boolean compareNonStringProps) throws IOException {
    if (a.hasProperties()) {
      Set<String> aPropNames = a.propertyNames();
      Set<String> bPropNames = b.propertyNames();
      Map<String, JsonNode> propsA = getObjectProps(a, aPropNames);
      Map<String, JsonNode> propsB = getObjectProps(b, bPropNames);
      return Jackson2Utils.compareJsonProperties(propsA, propsB, compareStringProps, compareNonStringProps);
    }
    return !b.hasProperties();
  }

  private static boolean hasSameObjectProps (AvroSchemaField a, AvroSchemaField b,boolean compareStringProps,

  boolean compareNonStringProps) throws IOException {
    return hasSameObjectProps(a.getSchema(), b.getSchema(), compareStringProps, compareNonStringProps);
  }

  private static Map<String, JsonNode> getObjectProps(AvroSchema schema, Set<String> propNames) throws IOException {
    Map<String, JsonNode> propMap = new HashMap<>(propNames.size());
    for (String propName : propNames) {
      propMap.put(propName, Jackson2Utils.toJsonNode(schema.getAllProps().getPropertyAsJsonLiteral(propName), false));
    }
    return propMap;
  }

  private static class SeenPair {
    private final AvroNamedSchema s1;
    private final AvroNamedSchema s2;

    public SeenPair(AvroNamedSchema s1, AvroNamedSchema s2) {
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
