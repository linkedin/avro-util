/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;


public class AvroRecordUtil {

  private AvroRecordUtil() {
    //utility class
  }

  /**
   * sets all fields who's value is not allowed according to their schema to their default values (as specified in their schema)
   * optionally throws if field is not set to a valid value yet schema has no default for it
   * @param record a record to recursively supplement defaults in
   * @param throwIfMissingValuesLeft true to throw if field value is invalid yet no default exists
   * @param <T> exact type of record
   * @return the input record
   */
  public static <T extends IndexedRecord> T supplementDefaults(T record, boolean throwIfMissingValuesLeft) {
    if (record == null) {
      throw new IllegalArgumentException("record argument required");
    }
    boolean isSpecific = AvroCompatibilityHelper.isSpecificRecord(record);
    //noinspection unchecked
    return (T) supplementDefaults(record, throwIfMissingValuesLeft, isSpecific);
  }

  /**
   * sets all fields who's value is not allowed according to their schema to their default values (as specified in their schema)
   * optionally throws if field is not set to a valid value yes schema has no default for it
   * @param record a record to DFS
   * @param throwIfMissingValuesLeft true to throw if field value is invalid yet no default exists
   * @param useSpecifics true to populate specific default values, false to populate with generics
   * @return the input record
   */
  private static IndexedRecord supplementDefaults(IndexedRecord record, boolean throwIfMissingValuesLeft, boolean useSpecifics) {
    Schema schema = record.getSchema();
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      Object fieldValue = record.get(field.pos());
      Schema fieldSchema = field.schema();

      boolean populate = true;
      if (AvroSchemaUtil.isValidValueForSchema(fieldValue, fieldSchema)) {
        //this field is fine
        populate = false;
      } else if (!AvroCompatibilityHelper.fieldHasDefault(field)) {
        //no default, yet no value. complain?
        if (throwIfMissingValuesLeft) {
          throw new IllegalArgumentException(schema.getName() + "."  + field.name()
              + " has no default value, yet no value is set on the input record");
        }
        populate = false;
      }

      if (populate) {
        if (useSpecifics) {
          fieldValue = AvroCompatibilityHelper.getSpecificDefaultValue(field);
        } else {
          fieldValue = AvroCompatibilityHelper.getGenericDefaultValue(field);
        }
        record.put(field.pos(), fieldValue);
      }

      if (fieldValue instanceof IndexedRecord) {
        supplementDefaults((IndexedRecord) fieldValue, throwIfMissingValuesLeft, useSpecifics);
      }
    }
    return record;
  }
}
