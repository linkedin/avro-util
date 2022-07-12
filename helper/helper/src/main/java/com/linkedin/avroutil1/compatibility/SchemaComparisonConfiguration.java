/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;


public class SchemaComparisonConfiguration {
  /**
   * behaves like avro &lt;= 1.7.2 - non-string props on fields or types are ignored
   */
  public static final SchemaComparisonConfiguration PRE_1_7_3 = new SchemaComparisonConfiguration(
      true, false, false, false, true, false
  );
  /**
   * same as {@link #STRICT} but allows int default values to match (round) float default values
   */
  public static final SchemaComparisonConfiguration LOOSE_NUMERICS = new SchemaComparisonConfiguration(
      true, true, true, true, true, true
  );
  public static final SchemaComparisonConfiguration STRICT = new SchemaComparisonConfiguration(
      true, true, true, false, true, true
  );

  private final boolean compareStringJsonProps;
  private final boolean compareNonStringJsonProps;
  private final boolean compareAliases;
  private final boolean compareIntToFloatDefaults;
  private final boolean compareFieldOrder;
  private final boolean compareFieldLogicalTypes;

  public SchemaComparisonConfiguration(
      boolean compareStringJsonProps,
      boolean compareNonStringJsonProps,
      boolean compareAliases,
      boolean compareIntToFloatDefaults,
      boolean compareFieldOrder,
      boolean compareFieldLogicalTypes
  ) {
    this.compareStringJsonProps = compareStringJsonProps;
    this.compareNonStringJsonProps = compareNonStringJsonProps;
    this.compareAliases = compareAliases;
    this.compareIntToFloatDefaults = compareIntToFloatDefaults;
    this.compareFieldOrder = compareFieldOrder;
    this.compareFieldLogicalTypes = compareFieldLogicalTypes;
  }

  public boolean isCompareStringJsonProps() {
    return compareStringJsonProps;
  }

  public boolean isCompareNonStringJsonProps() {
    return compareNonStringJsonProps;
  }

  public boolean isCompareAliases() {
    return compareAliases;
  }

  public boolean isCompareIntToFloatDefaults() {
    return compareIntToFloatDefaults;
  }

  public boolean isCompareFieldOrder() {
    return compareFieldOrder;
  }

  public boolean isCompareFieldLogicalTypes() {
    return compareFieldLogicalTypes;
  }
}


