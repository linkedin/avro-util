/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import com.linkedin.avro.compatibility.AvroVersion;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import org.json.JSONObject;


public class LegacySchemaTestUtil {
  private LegacySchemaTestUtil() {
    //util class
  }

  public static String apply(String schemaJson, Iterable<SchemaTransformStep> steps) throws Exception {
    String fixedSchema = schemaJson;
    for (SchemaTransformStep step : steps) {
      fixedSchema = apply(fixedSchema, step);
    }
    return fixedSchema;
  }

  public static String apply(String schemaJson, SchemaTransformStep step) throws Exception {
    String fixedSchema = step.applyToSchema(schemaJson);
    assertValid14Schema(fixedSchema);
    return fixedSchema;
  }

  public static void assertValid14Schema(String schemaJson) throws Exception {
    new JSONObject(schemaJson); //make sure its valid json
    if (AvroCompatibilityHelper.getRuntimeAvroVersion().equals(AvroVersion.AVRO_1_4)) {
      Schema.parse(schemaJson);//make sure this is valid under 1.4, if possible
    }
  }

  public static void assertValidSchema(String schemaJson) throws Exception {
    Schema.parse(schemaJson);
  }

  public static int countMatches(String haystack, String needle) {
    Pattern pattern = Pattern.compile(needle);
    Matcher matcher = pattern.matcher(haystack);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }
}
