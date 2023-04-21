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
import com.linkedin.avroutil1.model.AvroSchemaDifference;
import com.linkedin.avroutil1.model.AvroSchemaDifferenceType;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.JsonPropertiesContainer;
import com.linkedin.avroutil1.model.SchemaOrRef;
import java.io.IOException;
import java.util.ArrayList;
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

  /**
   * Compares two AvroSchema objects for equality using the specified SchemaComparisonConfiguration. This method is high
   * performant and return false on the first found mismatch, and if there is no mismatch, it returns true.
   *
   * @param a The first {@link AvroSchema} to compare.
   * @param b The second {@link AvroSchema} to compare.
   * @param config The {@link SchemaComparisonConfiguration} that specifies the configuration for the comparison.
   * @return true if the AvroSchema objects are equal according to the specified comparison configuration, false otherwise.
   */
  public static boolean equals(AvroSchema a, AvroSchema b, SchemaComparisonConfiguration config) {
    return equals(a, b, config, true).isEmpty();
  }

  /**
   * Compares two AvroSchema objects and finds the differences between them using the specified SchemaComparisonConfiguration.
   * This method will return a list of all the differences while comparing the schemas.
   *
   * @param a The first {@link AvroSchema} to compare.
   * @param b The second {@link AvroSchema} to compare.
   * @param config The {@link SchemaComparisonConfiguration} that specifies the configuration for the comparison.
   * @return List of {@link AvroSchemaDifference} objects representing the differences between the two AvroSchema objects
   */
  public static List<AvroSchemaDifference> findDifference(AvroSchema a, AvroSchema b,
      SchemaComparisonConfiguration config) {
    return equals(a, b, config, false);
  }

  private static List<AvroSchemaDifference> equals(AvroSchema a, AvroSchema b, SchemaComparisonConfiguration config,
      boolean fastFail) {
    validateConfig(config);
    Set<SeenPair> seen = new HashSet<>(3);
    List<AvroSchemaDifference> differences = new ArrayList<>();
    try {
      equals(a, b, config, seen, differences, fastFail);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return differences;
  }

  private static void validateConfig(SchemaComparisonConfiguration config) {
    if (config == null) {
      throw new IllegalArgumentException("config required");
    }
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelperCommon.getRuntimeAvroVersion();
    if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_7) && config.isCompareNonStringJsonProps()) {
      //1.7 itself changes between < 1.7.3 and >= 1.7.3, so we leave that validation to later runtime :-(
      throw new IllegalArgumentException(
          "avro " + runtimeAvroVersion + " does not preserve non-string props and so cannot compare them");
    }
  }

  private static void equals(AvroSchema a, AvroSchema b, SchemaComparisonConfiguration config, Set<SeenPair> seen,
      List<AvroSchemaDifference> differences, boolean fastFail) throws IOException {
    if (a == null && b == null) {
      return;
    }
    if (a == null || b == null) {
      differences.add(new AvroSchemaDifference(null, null, AvroSchemaDifferenceType.NULL_SCHEMA,
          "Either schemaA or schemaB is null"));
      return;
    }
    AvroType type = a.type();
    if (!Objects.equals(type, b.type())) {
      differences.add(
          new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(), AvroSchemaDifferenceType.TYPE_MISMATCH,
              String.format("Type %s in schemaA does not match with with type %s in schemaB", a, b)));
      return;
    }

    boolean considerJsonStringProps = config.isCompareStringJsonProps();
    boolean considerJsonNonStringProps = config.isCompareNonStringJsonProps();
    boolean considerJsonProps = considerJsonStringProps || considerJsonNonStringProps;

    if (considerJsonProps && !hasSameObjectProps(a, b, considerJsonStringProps,
        considerJsonNonStringProps)) {
      AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
          AvroSchemaDifferenceType.JSON_PROPERTY_MISMATCH,
          String.format("Json properties of %s in schemaA does not match with the json properties of %s in schemaB",
              a, b));
      differences.add(difference);
      if (fastFail) {
        return;
      }
    }

    switch (type) {
      // primitive types (all of these have nothing more to compare apart from their types)
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
        break;
      // named types
      case ENUM:
        findEnumSchemaDifferences((AvroEnumSchema) a, (AvroEnumSchema) b, config, seen, differences, fastFail);
        break;
      case FIXED:
        findFixedSchemaDifferences((AvroFixedSchema) a, (AvroFixedSchema) b, config, seen, differences, fastFail);
        break;
      case RECORD:
        recordSchemaEquals((AvroRecordSchema) a, (AvroRecordSchema) b, config, seen, differences, fastFail);
        break;
      // collections and union
      case ARRAY:
        equals(((AvroArraySchema) a).getValueSchema(), ((AvroArraySchema) b).getValueSchema(), config, seen,
            differences, fastFail);
        break;
      case MAP:
        equals(((AvroMapSchema) a).getValueSchema(), ((AvroMapSchema) b).getValueSchema(), config, seen, differences,
            fastFail);
        break;
      case UNION:
        findUnionSchemaDifferences((AvroUnionSchema) a, (AvroUnionSchema) b, config, seen, differences, fastFail);
        break;
      default:
        throw new IllegalStateException("Unhandled type: " + type);
    }
  }

  private static void findUnionSchemaDifferences(AvroUnionSchema a, AvroUnionSchema b,
      SchemaComparisonConfiguration config, Set<SeenPair> seen, List<AvroSchemaDifference> differences,
      boolean fastFail) throws IOException {
    List<SchemaOrRef> aBranches = a.getTypes();
    List<SchemaOrRef> bBranches = b.getTypes();
    if (aBranches.size() != bBranches.size()) {
      AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
          AvroSchemaDifferenceType.UNION_SIZE_MISMATCH,
          String.format("Size for union field %d from schemaA does not match with size for union field %d from schemaB",
              aBranches.size(), bBranches.size()));
      differences.add(difference);
      return;
    }
    // We check for the union branches only if the size is equal
    for (int i = 0; i < aBranches.size(); i++) {
      SchemaOrRef aBranch = aBranches.get(i);
      SchemaOrRef bBranch = bBranches.get(i);
      equals(aBranch, bBranch, config, seen, differences, fastFail);
    }
  }

  private static void equals(SchemaOrRef aBranch, SchemaOrRef bBranch, SchemaComparisonConfiguration config,
      Set<SeenPair> seen, List<AvroSchemaDifference> differences, boolean fastFail) throws IOException {
    if (aBranch.getSchema() != null) {
      equals(aBranch.getSchema(), bBranch.getSchema(), config, seen, differences, fastFail);
    }
    if (aBranch.getRef() != null && !aBranch.getRef().equals(bBranch.getRef())) {
      AvroSchemaDifference difference = new AvroSchemaDifference(aBranch.getCodeLocation(), bBranch.getCodeLocation(),
          AvroSchemaDifferenceType.SCHEMA_REFERENCE_MISMATCH,
          String.format("Schema ref %s in schemaA does not match with the schema ref %s in schemaB", aBranch.getRef(),
              bBranch.getRef()));
      differences.add(difference);
    }
  }

  private static void findEnumSchemaDifferences(AvroEnumSchema a, AvroEnumSchema b,
      SchemaComparisonConfiguration config, Set<SeenPair> seen, List<AvroSchemaDifference> differences,
      boolean fastFail) {
    // Compare fullName of enum fields in schemaA and schemaB
    String enumFullNameA = a.getFullName();
    String enumFullNameB = b.getFullName();
    if (!enumFullNameA.equals(enumFullNameB)) {
      AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
          AvroSchemaDifferenceType.ENUM_NAME_MISMATCH,
          String.format("Name for Enum field %s in schemaA does not match with name enum field %s in schemaB",
              enumFullNameA, enumFullNameB));
      differences.add(difference);
      if (fastFail) {
        return;
      }
    }

    // Compare aliases of enum fields in schemaA and schemaB
    if (config.isCompareAliases() && !hasSameAliases(a, b)) {
      AvroSchemaDifference difference =
          new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(), AvroSchemaDifferenceType.ALIASES_MISMATCH,
              String.format(
                  "Aliases for enum field %s in schemaA does not match with aliases for enum field %s in schemaB",
                  enumFullNameA, enumFullNameB));
      differences.add(difference);
      if (fastFail) {
        return;
      }
    }

    // Compare symbols of enum fields in schemaA and schemaB
    if (!a.getSymbols().equals(b.getSymbols())) {
      AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
          AvroSchemaDifferenceType.ENUM_SYMBOL_MISMATCH, String.format(
          "Symbols %s of enum field %s in schemaA does not match with symbols %s of enum field %s in schemaB",
          a.getSymbols().toString(), enumFullNameA, b.getSymbols().toString(), enumFullNameB));
      differences.add(difference);
    }
  }

  private static void findFixedSchemaDifferences(AvroFixedSchema a, AvroFixedSchema b,
      SchemaComparisonConfiguration config, Set<SeenPair> seen, List<AvroSchemaDifference> differences,
      boolean fastFail) {
    // Compare fullName of fixed fields in schemaA and schemaB
    String fixedFullNameA = a.getFullName();
    String fixedFullNameB = b.getFullName();
    if (!fixedFullNameA.equals(fixedFullNameB)) {
      AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
          AvroSchemaDifferenceType.FIXED_NAME_MISMATCH,
          String.format("Name for fixed field %s in schemaA does not match with name of fixed field %s in schemaB",
              fixedFullNameA, fixedFullNameB));
      differences.add(difference);
      if (fastFail) {
        return;
      }
    }

    // Compare aliases of fixed fields in schemaA and schemaB
    if (config.isCompareAliases() && !hasSameAliases(a, b)) {
      AvroSchemaDifference difference =
          new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(), AvroSchemaDifferenceType.ALIASES_MISMATCH,
              String.format(
                  "Aliases for fixed field %s in schemaA does not match with the aliases for fixed field %s in schemaB",
                  fixedFullNameA, fixedFullNameB));
      differences.add(difference);
      if (fastFail) {
        return;
      }
    }

    // Compare size of fixed fields in schemaA and schemaB
    if ((a.getSize() != b.getSize())) {
      AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
          AvroSchemaDifferenceType.FIXED_SIZE_MISMATCH,
          String.format("Size for fixed field %s in schemaA does not match with the size for fixed field %s in schemaB",
              fixedFullNameA, fixedFullNameB));
      differences.add(difference);
    }
  }

  private static void recordSchemaEquals(AvroRecordSchema a, AvroRecordSchema b, SchemaComparisonConfiguration config,
      Set<SeenPair> seen, List<AvroSchemaDifference> differences, boolean fastFail) throws IOException {
    if (!a.getFullName().equals(b.getFullName())) {
      AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
          AvroSchemaDifferenceType.RECORD_NAME_MISMATCH,
          String.format("Name of the %s in schemaA does not match with name of the %s in schemaB", a, b));
      differences.add(difference);
      if (fastFail) {
        return;
      }
    }
    // loop protection for self-referencing schemas
    SeenPair pair = new SeenPair(a, b);
    if (seen.contains(pair)) {
      return;
    }
    seen.add(pair);

    boolean considerJsonStringProps = config.isCompareStringJsonProps();
    boolean considerJsonNonStringProps = config.isCompareNonStringJsonProps();
    boolean considerAliases = config.isCompareAliases();
    boolean considerJsonProps = considerJsonStringProps || considerJsonNonStringProps;

    try {
      if (considerAliases && !hasSameAliases(a, b)) {
        AvroSchemaDifference difference = new AvroSchemaDifference(a.getCodeLocation(), b.getCodeLocation(),
            AvroSchemaDifferenceType.ALIASES_MISMATCH,
            String.format("Aliases for %s in schemaA does not match with the aliases for %s in schemaB", a, b));
        differences.add(difference);
        if (fastFail) {
          return;
        }
      }

      List<AvroSchemaField> aFields = a.getFields();
      List<AvroSchemaField> bFields = b.getFields();
      int numberOfCommonFields = Math.min(aFields.size(), bFields.size());
      for (int i = 0; i < numberOfCommonFields; i++) {
        if (fastFail && !differences.isEmpty()) {
          return;
        }
        AvroSchemaField aField = aFields.get(i);
        AvroSchemaField bField = bFields.get(i);
        if (!aField.getName().equals(bField.getName())) {
          AvroSchemaDifference difference = new AvroSchemaDifference(aField.getCodeLocation(), bField.getCodeLocation(),
              AvroSchemaDifferenceType.RECORD_FIELD_NAME_MISMATCH,
              String.format("Name for field %s in %s in schemaA does not match with field %s in %s in schemaB",
                  aField.getName(), a, bField.getName(), b));
          differences.add(difference);
          if (fastFail) {
            return;
          }
        }
        equals(aField.getSchema(), bField.getSchema(), config, seen, differences, fastFail);

        if (aField.hasDefaultValue() && bField.hasDefaultValue()) {
          if (!aField.getDefaultValue().equals(bField.getDefaultValue())) {
            AvroSchemaDifference difference =
                new AvroSchemaDifference(aField.getCodeLocation(), bField.getCodeLocation(),
                    AvroSchemaDifferenceType.RECORD_DEFAULT_VALUE_MISMATCH, String.format(
                    "Default value %s for field %s in %s in schemaA does not match with default value %s field %s in %s in schemaB",
                    aField.getDefaultValue(), aField.getName(), a, bField.getDefaultValue(), bField.getName(), b));
            differences.add(difference);
            if (fastFail) {
              return;
            }
          }
        } else if (aField.hasDefaultValue() || bField.hasDefaultValue()) {
          AvroSchemaDifference difference = new AvroSchemaDifference(aField.getCodeLocation(), bField.getCodeLocation(),
              AvroSchemaDifferenceType.DEFAULT_VALUE_MISMATCH,
              String.format("Either field %s in %s in schemaA or field %s in %s in schemaB has missing default value",
                  aField.getName(), a, bField.getName(), b));
          differences.add(difference);
          if (fastFail) {
            return;
          }
        }
        if (!Objects.equals(aField.getPosition(), bField.getPosition())) {
          AvroSchemaDifference difference = new AvroSchemaDifference(aField.getCodeLocation(), bField.getCodeLocation(),
              AvroSchemaDifferenceType.RECORD_FIELD_POSITION_MISMATCH, String.format(
              "Position %d for field %s in %s in schemaA does not match with position %d field %s in %s in schemaB",
              aField.getPosition(), aField.getName(), a, bField.getPosition(), bField.getName(), b));
          differences.add(difference);
          if (fastFail) {
            return;
          }
        }
        if (considerJsonProps && !hasSameObjectProps(aField, bField, considerJsonStringProps,
            considerJsonNonStringProps)) {
          AvroSchemaDifference difference = new AvroSchemaDifference(aField.getCodeLocation(), bField.getCodeLocation(),
              AvroSchemaDifferenceType.JSON_PROPERTY_MISMATCH,
              String.format("Json properties of field \"%s\" in schemaA does not match with the json properties in schemaB",
                  aField.getName(), bField.getName()));
          differences.add(difference);
          if (fastFail) {
            return;
          }
        }
        if (considerAliases && !hasSameAliases(aField, bField)) {
          AvroSchemaDifference difference = new AvroSchemaDifference(aField.getCodeLocation(), bField.getCodeLocation(),
              AvroSchemaDifferenceType.ALIASES_MISMATCH,
              String.format("Aliases for %s in schemaA does not match with the aliases for %s in schemaB", a, b));
          differences.add(difference);
          if (fastFail) {
            return;
          }
        }
      }
      // If there is size mismatch between the schemas, we add the additional fields in the differences
      for (int i = numberOfCommonFields; i < aFields.size(); i++) {
        AvroSchemaField aField = aFields.get(i);
        AvroSchemaDifference difference =
            new AvroSchemaDifference(aField.getCodeLocation(), null, AvroSchemaDifferenceType.ADDITIONAL_FIELD,
                String.format("Additional field %s found in schemaA", aField.getName()));
        differences.add(difference);
        if (fastFail) {
          return;
        }
      }
      for (int i = numberOfCommonFields; i < bFields.size(); i++) {
        AvroSchemaField bField = bFields.get(i);
        AvroSchemaDifference difference =
            new AvroSchemaDifference(null, bField.getCodeLocation(), AvroSchemaDifferenceType.ADDITIONAL_FIELD,
                String.format("Additional field %s found in schemaB", bField.getName()));
        differences.add(difference);
        if (fastFail) {
          return;
        }
      }
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

  private static boolean hasSameObjectProps(JsonPropertiesContainer aPropContainer,
      JsonPropertiesContainer bPropContainer, boolean compareStringProps, boolean compareNonStringProps)
      throws IOException {
    if (aPropContainer.hasProperties()) {
      Map<String, JsonNode> aProperties = getObjectProps(aPropContainer, aPropContainer.propertyNames());
      Map<String, JsonNode> bProperties = getObjectProps(bPropContainer, bPropContainer.propertyNames());
      return Jackson2Utils.compareJsonProperties(aProperties, bProperties, compareStringProps,
          compareNonStringProps);
    } return !bPropContainer.hasProperties();
  }

  private static Map<String, JsonNode> getObjectProps(JsonPropertiesContainer jsonPropertiesContainer,
      Set<String> propNames) throws IOException {
    Map<String, JsonNode> propMap = new HashMap<>(propNames.size());
    for (String propName : propNames) {
      propMap.put(propName,
          Jackson2Utils.toJsonNode(jsonPropertiesContainer.getPropertyAsJsonLiteral(propName), false));
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
