/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


//package-protected ON PURPOSE
class RemoveDuplicateStep extends AbstractSchemaTraversingStep {

  private final String _duplicate;

  RemoveDuplicateStep(Exception cause, String duplicate) {
    super(cause);
    if (duplicate == null || duplicate.trim().isEmpty()) {
      throw new IllegalArgumentException(String.format("illegal value for parameter \"%s\"", duplicate));
    }
    _duplicate = duplicate;
  }

  @Override
  public SchemaTransformStep inverse() {
    return new AbstractSchemaTransformStep(_cause) {

      @Override
      public String applyToText(String text) {
        return text; //nop
      }

      @Override
      public SchemaTransformStep inverse() {
        return RemoveDuplicateStep.this;
      }
    };
  }

  @Override
  public String toString() {
    return "remove duplicate field " + _duplicate;
  }

  @Override
  protected void visitRecordObject(JSONObject recordObject, List<String> path) throws JSONException {
    JSONArray fieldsArray = recordObject.getJSONArray("fields");
    Map<String, List<JSONObject>> fieldNamesToDefinitions = new HashMap<>();

    //1st do a shallow iteration over immediate fields, see if any of them are dups
    for (int i = 0; i < fieldsArray.length(); i++) {
      JSONObject fieldObject = fieldsArray.getJSONObject(i);
      String fieldName = fieldObject.getString("name");
      fieldNamesToDefinitions.computeIfAbsent(fieldName, s -> new ArrayList<>(1)).add(fieldObject);
    }

    //remove any dups among our immediate fields, also traverse into any complex types that may hold further dups
    for (Map.Entry<String, List<JSONObject>> fieldEntry : fieldNamesToDefinitions.entrySet()) {
      String fieldName = fieldEntry.getKey();
      List<JSONObject> definitions = fieldEntry.getValue();
      JSONObject fieldDefinition = definitions.get(0); //TODO - figure out if 1.4 keeps 1st or last or what
      if (definitions.size() > 1 && _duplicate.equals(fieldName)) { //dedup this field. leave only fieldDefinition
        //because the Json.org API is so awesome we have to rebuild the entire fields array sans the dups
        //and drop the new fields array in place of the old one
        JSONArray newFieldsArray = new JSONArray();
        for (int i = 0; i < fieldsArray.length(); i++) {
          JSONObject fieldObject = fieldsArray.getJSONObject(i);
          if (!definitions.contains(fieldObject) || fieldObject == fieldDefinition) {
            newFieldsArray.put(fieldObject); //this is an append. wonderful API ...
          }
        }
        definitions.clear();
        definitions.add(fieldDefinition);
        recordObject.put("fields", newFieldsArray);
        fieldsArray = newFieldsArray;
      }
    }
  }
}
