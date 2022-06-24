/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Objects;


/**
 * various configuration parameters used by various avro versions when parsing schemas
 */
public class SchemaParseConfiguration {
  public final static SchemaParseConfiguration STRICT = new SchemaParseConfiguration(
      true, true, true
  );
  public final static SchemaParseConfiguration LOOSE_NUMERICS = new SchemaParseConfiguration(
      true, true, false
  );
  public final static SchemaParseConfiguration LOOSE = new SchemaParseConfiguration(
      false, false, false
  );

  /**
   * validate that names for named types (records, enums, fixed types) and fields are
   * valid identifiers according to the avro specification (same rules as java identifiers).
   * natively supported under avro 1.5+
   */
  private final boolean validateNames;
  /**
   * validate that default values for fields match the declared field type, and that defaults
   * for union fields match the 1st branch in the union. tolerates ints as default values for
   * floating point types and vice versa.
   * natively supported under avro 1.7+
   */
  private final boolean validateDefaultValues;
  /**
   * for default numeric values (ints, floats, etc) checks that the default value type STRICTLY
   * matches the field type - so 0.0 is not a valid default for ints and 0 not valid for floats.
   * requires that validateDefaultValues above be enabled.
   * no version of avro currently natively supports this validation.
   */
  private final boolean validateNumericDefaultValueTypes;

  public SchemaParseConfiguration(
      boolean validateNames,
      boolean validateDefaultValues,
      boolean validateNumericDefaultValueTypes
  ) {
    if (validateNumericDefaultValueTypes && !validateDefaultValues) {
      throw new IllegalArgumentException("validateNumericDefaultValueTypes requires validateDefaultValues");
    }
    this.validateNames = validateNames;
    this.validateDefaultValues = validateDefaultValues;
    this.validateNumericDefaultValueTypes = validateNumericDefaultValueTypes;
  }

  @Deprecated
  public SchemaParseConfiguration(boolean validateNames, boolean validateDefaultValues) {
    this(validateNames, validateDefaultValues, validateDefaultValues);
  }

  public boolean validateNames() {
    return validateNames;
  }

  public boolean validateDefaultValues() {
    return validateDefaultValues;
  }

  public boolean validateNumericDefaultValueTypes() {
    return validateNumericDefaultValueTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaParseConfiguration that = (SchemaParseConfiguration) o;
    return validateNames == that.validateNames && validateDefaultValues == that.validateDefaultValues
        && validateNumericDefaultValueTypes == that.validateNumericDefaultValueTypes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(validateNames, validateDefaultValues, validateNumericDefaultValueTypes);
  }
}
