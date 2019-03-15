/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;


/**
 * this class represents a malformed avro schema that unfortunately exists due to legacy.
 * this class contains the original schema json, a "fixed" schema (which is usable by modern
 * avro) and the list of transforms required to get from one to the other.
 * <p>
 * issues with legacy schemas
 * <ul>
 *   <li>illegal characters in identifier names and enum values - things like '@' '-' and spaces</li>
 *   <li>duplicate properties - same property defined multiple times. identical definitions if were lucky...</li>
 *   <li>invalid default property values (like a boolean default value for an int prop)</li>
 *   <li>sometimes the schema definition may not even be valid json - in the olden days people would write those and register them manually</li>
 * </ul>
 * <p>
 * some of these issues are fixable - by escaping illegal characters and removing duplicate definitions.
 * this can allow us to continue "supporting" those schemas by using a fixing up the schema and using
 * the fixed schema and the list of fix steps to continue read/writing both the schema itself as well
 * as data written using it in a backwards-compatible way.
 */
public class LegacyAvroSchema {
  private String _originalSchemaId;    //id of the original schema in string format. null for unknown
  private String _originalSchemaJson;  //as obtained from somewhere (likely schema registry), potentially minified
  private Exception _issue;            //the (1st) thing wrong with the original schema json
  private Schema _fixedSchema;         //a fixed version of the schema, usable by modern avro. null if not fixable
  private List<SchemaTransformStep> _transforms; //list of steps (in order) required to make schema usable
  private List<SchemaTransformStep> _inverseTransforms; //inverse of above

  public LegacyAvroSchema(String originalSchemaId, String originalSchemaJson) {
    _originalSchemaId = originalSchemaId;
    _originalSchemaJson = originalSchemaJson;
    //we want to see the issue (if any) with our own eyes instead of letting the user set it - no trust
    try {
      Schema parsed = AvroCompatibilityHelper.parse(originalSchemaJson);
      //schema is actually fine
      _issue = null;
      _fixedSchema = parsed;
      _transforms = Collections.emptyList();
      _inverseTransforms = Collections.emptyList();
    } catch (Exception e) {
      _issue = e;
      //see if we can minify and fix the schema (those are 2 different outputs)
      SchemaFixResults fixResults = LegacyAvroSchemaUtil.tryFix(originalSchemaJson);
      _fixedSchema = fixResults.getFixedSchema(); //might be null
      if (_fixedSchema != null) {
        _transforms = fixResults.getStepsTaken();
        _inverseTransforms = _transforms.stream().map(SchemaTransformStep::inverse).collect(Collectors.toList());
        Collections.reverse(_inverseTransforms);
      }
    }
  }

  public String getOriginalSchemaId() {
    return _originalSchemaId;
  }

  public String getOriginalSchemaJson() {
    return _originalSchemaJson;
  }

  public Exception getIssue() {
    return _issue;
  }

  public Schema getFixedSchema() {
    return _fixedSchema;
  }

  public List<SchemaTransformStep> getTransforms() {
    return _transforms;
  }

  public List<SchemaTransformStep> getInverseTransforms() {
    return _inverseTransforms;
  }

  public GenericRecord deserializeJson(String json) {
    return LegacyAvroSchemaUtil.deserializeJson(this, json);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LegacyAvroSchema that = (LegacyAvroSchema) o;
    //all other fields are logical derivatives of the schema string
    return Objects.equals(_originalSchemaId, that._originalSchemaId)
        && Objects.equals(_originalSchemaJson, that._originalSchemaJson);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_originalSchemaId, _originalSchemaJson);
  }
}
