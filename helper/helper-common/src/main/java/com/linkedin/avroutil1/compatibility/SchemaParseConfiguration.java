package com.linkedin.avroutil1.compatibility;

import java.util.Objects;


/**
 * various configuration parameters used by various avro versions when parsing schemas
 */
public class SchemaParseConfiguration {
  public final static SchemaParseConfiguration STRICT = new SchemaParseConfiguration(true, true);
  public final static SchemaParseConfiguration LOOSE = new SchemaParseConfiguration(true, true);

  /**
   * validate that names for named types (records, enums, fixed types, fields) are
   * valid identifiers according to the avro specification. natively supported under avro 1.5+
   */
  private final boolean validateNames;
  /**
   * validate that default values for fields are correct. natively supported under avro 1.7+
   */
  private final boolean validateDefaultValues;

  public SchemaParseConfiguration(boolean validateNames, boolean validateDefaultValues) {
    this.validateNames = validateNames;
    this.validateDefaultValues = validateDefaultValues;
  }

  public boolean validateNames() {
    return validateNames;
  }

  public boolean validateDefaultValues() {
    return validateDefaultValues;
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
    return validateNames == that.validateNames && validateDefaultValues == that.validateDefaultValues;
  }

  @Override
  public int hashCode() {
    return Objects.hash(validateNames, validateDefaultValues);
  }
}
