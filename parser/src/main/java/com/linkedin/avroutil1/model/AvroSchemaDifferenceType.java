package com.linkedin.avroutil1.model;

/**
 * Enum representing Avro schema difference types.
 */
public enum AvroSchemaDifferenceType {

  /**
   * Null schema provided.
   */
  NULL_SCHEMA,

  /**
   * Schema reference mismatch between schema A and schema B.
   */
  SCHEMA_REFERENCE_MISMATCH,

  /**
   * Type mismatch between schema A and schema B.
   */
  TYPE_MISMATCH,

  /**
   * Aliases mismatch between schema A and schema B.
   */
  ALIASES_MISMATCH,

  /**
   * JSON property count mismatch between schema A and schema B.
   */
  JSON_PROPERTY_COUNT_MISMATCH,

  /**
   * JSON property mismatch between schema A and schema B.
   */
  JSON_PROPERTY_MISMATCH,

  /**
   * Enum name mismatch between schema A and schema B.
   */
  ENUM_NAME_MISMATCH,

  /**
   * Enum symbol mismatch between schema A and schema B.
   */
  ENUM_SYMBOL_MISMATCH,

  /**
   * Fixed name mismatch between schema A and schema B.
   */
  FIXED_NAME_MISMATCH,

  /**
   * Fixed size mismatch between schema A and schema B.
   */
  FIXED_SIZE_MISMATCH,

  /**
   * Union size mismatch between schema A and schema B.
   */
  UNION_SIZE_MISMATCH,

  /**
   * Record name mismatch between schema A and schema B.
   */
  RECORD_NAME_MISMATCH,

  /**
   * Record field count mismatch between schema A and schema B.
   */
  RECORD_FIELD_COUNT_MISMATCH,

  /**
   * Record field name mismatch between schema A and schema B.
   */
  RECORD_FIELD_NAME_MISMATCH,

  /**
   * Record default value mismatch between schema A and schema B.
   */
  RECORD_DEFAULT_VALUE_MISMATCH,

  /**
   * Default value missing in schema A or schema B.
   */
  DEFAULT_VALUE_MISSING,

  /**
   * Record field position mismatch between schema A and schema B.
   */
  RECORD_FIELD_POSITION_MISMATCH,

  /**
   * Additional field found in schema A or schema B.
   */
  ADDITIONAL_FIELD,

  /**
   * Unknown difference type.
   */
  UNKNOWN

}
