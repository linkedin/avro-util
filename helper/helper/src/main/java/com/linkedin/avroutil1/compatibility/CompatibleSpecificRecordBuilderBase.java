/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */


package com.linkedin.avroutil1.compatibility;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;


public class CompatibleSpecificRecordBuilderBase {

  private static final Schema.Field[] EMPTY_FIELDS = new Schema.Field[0];
  private final Schema schema;
  private final Schema.Field[] fields;
  private final boolean[] fieldSetFlags;

  protected CompatibleSpecificRecordBuilderBase (Schema schema) {
    this.schema = schema;
    fields = schema.getFields().toArray(EMPTY_FIELDS);
    fieldSetFlags = new boolean[fields.length];
  }

  protected final Schema schema() {
    return schema;
  }

  protected final Schema.Field[] fields() {
    return fields;
  }

  protected final boolean[] fieldSetFlags() {
    return fieldSetFlags;
  }

  /**
   * Validates that a particular value for a given field is valid according to the
   * following algorithm: 1. If the value is not null, or the field type is null,
   * or the field type is a union which accepts nulls, returns. 2. Else, if the
   * field has a default value, returns. 3. Otherwise throws AvroRuntimeException.
   *
   * @param field the field to validate.
   * @param value the value to validate.
   * @throws NullPointerException if value is null and the given field does not
   *                              accept null values.
   */
  protected void validate(Schema.Field field, Object value) {
    if (!isValidValue(field, value) && AvroCompatibilityHelper.getSpecificDefaultValue(field) == null) {
      throw new AvroRuntimeException("Field " + field + " does not accept null values");
    }
  }

  /**
   * Tests whether a value is valid for a specified field.
   *
   * @param f     the field for which to test the value.
   * @param value the value to test.
   * @return true if the value is valid for the given field; false otherwise.
   */
  protected static boolean isValidValue(Schema.Field f, Object value) {
    if (value != null) {
      return true;
    }

    Schema schema = f.schema();
    Schema.Type type = schema.getType();

    // If the type is null, any value is valid
    if (type == Schema.Type.NULL) {
      return true;
    }

    // If the type is a union that allows nulls, any value is valid
    if (type == Schema.Type.UNION) {
      for (Schema s : schema.getTypes()) {
        if (s.getType() == Schema.Type.NULL) {
          return true;
        }
      }
    }

    // The value is null but the type does not allow nulls
    return false;
  }

  @SuppressWarnings("unchecked")
  protected  <T> T deepCopyField (Object inputValue, Schema fieldSchema,
      String stringRepresentationValue) {
    RecordConversionConfig allowAllConfig = RecordConversionConfig.ALLOW_ALL_USE_UTF8;

    RecordConversionContext context = new RecordConversionContext(new RecordConversionConfig(
        allowAllConfig.isUseAliasesOnNamedTypes(),
        allowAllConfig.isUseAliasesOnFields(),
        allowAllConfig.isValidateAliasUniqueness(),
        allowAllConfig.isUseEnumDefaults(),
        StringRepresentation.valueOf(stringRepresentationValue),
        allowAllConfig.isUseStringRepresentationHints()
    ));

    context.setUseSpecifics(true);
    context.setClassLoader(Thread.currentThread().getContextClassLoader());

    return (T) AvroRecordUtil.deepConvert(inputValue, fieldSchema, fieldSchema, context,
        StringRepresentation.valueOf(stringRepresentationValue));
  }
}
