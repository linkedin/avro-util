/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;


//package-private ON PURPOSE

/**
 * this class holds the results of attempting to fix a given avsc schema json string.
 * there are 3 separate "output" artifacts from an attempt:
 * <ul>
 *   <li>
 *     the minified json schema - this is the input schema with all whitespaces and line breaks stripped out.
 *     this depends on the input being at least valid json.
 *   </li>
 *   <li>
 *     the fixed avro schema. beyond being valid json, if we can "fix" the input schema to a point where
 *     avro can parse and use it, we can produce a fixed schema and the list of steps applied to the input
 *     to arrive at this fixed schema.
 *   </li>
 *   <li>
 *     the "issue" (exception) preventing us from making any further progress. this can be a json correctness
 *     issue (in which case we wont have a minified json or fixed schema), or some issue further along preventing
 *     us from fixing a schema.
 *   </li>
 * </ul>
 */
class SchemaFixResults {
  private final String originalSchemaJson; //as provided by user
  private final String minifiedOriginalSchemaJson; //stripped of whitespaces and carriage returns (if valid json)
  //following are != null if schema json was fixable
  private final Schema fixedSchema;
  private final List<SchemaTransformStep> stepsTaken;
  private final Throwable issue; // != null if unfixable

  private SchemaFixResults(
      String originalSchemaJson,
      String minifiedOriginalSchemaJson,
      Schema fixedSchema,
      List<SchemaTransformStep> stepsTaken,
      Throwable issue
  ) {
    this.originalSchemaJson = originalSchemaJson;
    this.minifiedOriginalSchemaJson = minifiedOriginalSchemaJson;
    this.fixedSchema = fixedSchema;
    this.stepsTaken = stepsTaken;
    this.issue = issue;
  }

  static SchemaFixResults valid(String originalSchema, Schema fixed) {
    return new SchemaFixResults(originalSchema, fixed.toString(false), fixed, Collections.emptyList(), null);
  }

  static SchemaFixResults fixable(String originalSchema, String minified, Schema fixed, List<SchemaTransformStep> steps) {
    return new SchemaFixResults(originalSchema, minified, fixed, steps, null);
  }

  static SchemaFixResults notJson(String originalSchema, Throwable issue) {
    return new SchemaFixResults(originalSchema, null, null, null, issue);
  }

  static SchemaFixResults unfixable(String originalSchema, String minified, Throwable issue) {
    return new SchemaFixResults(originalSchema, minified, null, null, issue);
  }

  public String getOriginalSchemaJson() {
    return originalSchemaJson;
  }

  public String getMinifiedOriginalSchemaJson() {
    return minifiedOriginalSchemaJson;
  }

  public Schema getFixedSchema() {
    return fixedSchema;
  }

  public List<SchemaTransformStep> getStepsTaken() {
    return stepsTaken;
  }

  public Throwable getIssue() {
    return issue;
  }
}
