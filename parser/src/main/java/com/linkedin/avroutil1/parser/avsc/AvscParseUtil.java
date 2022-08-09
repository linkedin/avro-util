/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroLiteral;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.parser.jsonpext.JsonValueExt;


public class AvscParseUtil {
  private AvscParseUtil() {
    //utility code
  }

  /**
   * utility method for parsing a field default value late (at some point after originally encountering the field).
   * this happens when we see the type definition for the field type after we see the field itself.
   * @param field a field with a previously-unresolved schema and (as a result) a not-yet-parsed default value
   * @param context the context under which the field was originally found
   */
  public static void lateParseFieldDefault(
      AvroSchemaField field,
      AvscFileParseContext context
  ) {
    AvroSchema fieldSchema = field.getSchema();
    AvroSchema defaultValueExpectedSchema = fieldSchema;
    if (fieldSchema.type() == AvroType.UNION) {
      defaultValueExpectedSchema = ((AvroUnionSchema) fieldSchema).getTypes().get(0).getSchema();
    }
    JsonValueExt defaultValueExpr = ((AvscUnparsedLiteral) field.getDefaultValue()).getDefaultValueNode();
    LiteralOrIssue defaultValueOrIssue = context.getParser().parseLiteral(
        defaultValueExpr,
        defaultValueExpectedSchema,
        field.getName(),
        context
    );
    if (defaultValueOrIssue.getIssue() == null) {
      //TODO - allow parsing default values that are branch != 0 (and add an issue)
      AvroLiteral parsedDefaultValue = defaultValueOrIssue.getLiteral();
      field.setDefaultValue(parsedDefaultValue);
    } else {
      context.addIssue(defaultValueOrIssue.getIssue());
    }
  }
}
