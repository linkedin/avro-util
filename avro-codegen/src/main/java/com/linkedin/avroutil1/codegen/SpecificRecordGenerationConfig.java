/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.model.AvroJavaStringRepresentation;


/**
 * configuration class for controlling the output of code generation
 */
public class SpecificRecordGenerationConfig {
  public final static SpecificRecordGenerationConfig BROAD_COMPATIBILITY = new SpecificRecordGenerationConfig(
      true,
      true,
      true,
      true,
      false,
      AvroJavaStringRepresentation.CHAR_SEQUENCE,
      AvroJavaStringRepresentation.STRING,
      AvroVersion.AVRO_1_4
  );

  /**
   * true to make generated fields public
   */
  private boolean publicFields;
  /**
   * true to generate (public) getters
   */
  private boolean getters;
  /**
   * true to generate (public) setters
   */
  private boolean setters;
  /**
   * true to generate builders
   */
  private boolean builders;
  /**
   * true to honor java.lang.String properties on string types
   */
  private boolean honorStringTypeHints;
  /**
   * type used for generated "string" fields (unless type hints exist and are not ignored)
   */
  private AvroJavaStringRepresentation defaultFieldStringRepresentation;
  /**
   * type used for generated "string" getters/setters (unless type hints exist and are not ignored)
   */
  private AvroJavaStringRepresentation defaultMethodStringRepresentation;
  /**
   * minimum ("major") version of avro the generated code is expected to work under
   */
  private AvroVersion minimumSupportedAvroVersion;

  public SpecificRecordGenerationConfig(
      boolean publicFields,
      boolean getters,
      boolean setters,
      boolean builders,
      boolean honorStringTypeHints,
      AvroJavaStringRepresentation defaultFieldStringRepresentation,
      AvroJavaStringRepresentation defaultMethodStringRepresentation,
      AvroVersion minimumSupportedAvroVersion
  ) {
    this.publicFields = publicFields;
    this.getters = getters;
    this.setters = setters;
    this.builders = builders;
    this.honorStringTypeHints = honorStringTypeHints;
    this.defaultFieldStringRepresentation = defaultFieldStringRepresentation;
    this.defaultMethodStringRepresentation = defaultMethodStringRepresentation;
    this.minimumSupportedAvroVersion = minimumSupportedAvroVersion;
  }

  public AvroVersion getMinimumSupportedAvroVersion() {
    return minimumSupportedAvroVersion;
  }

  public boolean hasPublicFields() {
    return publicFields;
  }

  public AvroJavaStringRepresentation getDefaultFieldStringRepresentation() {
    return defaultFieldStringRepresentation;
  }

  public AvroJavaStringRepresentation getDefaultMethodStringRepresentation() {
    return defaultMethodStringRepresentation;
  }
}
