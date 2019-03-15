/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public abstract class AbstractSchemaTraversingStep extends AbstractSchemaTransformStep {

  protected AbstractSchemaTraversingStep(Exception cause) {
    super(cause);
  }

  @Override
  public String applyToSchema(String schemaJson) {
    try {
      JSONObject obj = new JSONObject(schemaJson);
      traverseRecordDefinition(obj, new ArrayList<>());
      return obj.toString(3); //pretty-print
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  protected void traverseRecordDefinition(JSONObject recordObject, List<String> path) throws JSONException {

    visitRecordObject(recordObject, path);

    JSONArray fieldsArray = recordObject.getJSONArray("fields");

    //recurse into fields
    for (int i = 0; i < fieldsArray.length(); i++) {
      JSONObject fieldObject = fieldsArray.getJSONObject(i);
      String fieldName = fieldObject.getString("name");
      Object fieldTypeDeclaration = fieldObject.get("type");

      path.add(fieldName); //push path
      traverseFieldTypeDeclaration(fieldTypeDeclaration, path);
      path.remove(path.size() - 1); //pop path
    }
  }

  protected void visitRecordObject(JSONObject recordObject, List<String> path) throws JSONException {
    //nop
  }


  private void traverseFieldTypeDeclaration(Object fieldTypeDeclaration, List<String> path) throws JSONException {
    if (fieldTypeDeclaration instanceof String) {
      return;
    } else if (fieldTypeDeclaration instanceof JSONArray) { //union
      JSONArray unionTypes = (JSONArray) fieldTypeDeclaration;
      for (int i = 0; i < unionTypes.length(); i++) {
        traverseFieldTypeDeclaration(unionTypes.get(i), path); //we dont append anything to path ON PURPOSE
      }
    } else if (fieldTypeDeclaration instanceof JSONObject) {
      //type is an inline type definition of some sort
      traverseTypeDefinition((JSONObject) fieldTypeDeclaration, path); //we dont append anything to path ON PURPOSE
    } else {
      throw new IllegalStateException("unhandled field type " + fieldTypeDeclaration);
    }
  }

  private void traverseTypeDefinition(JSONObject typeObject, List<String> path) throws JSONException {
    String type = typeObject.getString("type");
    switch (type) {
      case "fixed":
        return;
      case "enum":
        return; //we do not do anything with enums as they cannot have fields
      case "array":
        Object itemsType = typeObject.get("items");
        traverseFieldTypeDeclaration(itemsType, path); //path unchanged
        return;
      case "map":
        Object valuesType = typeObject.get("values");
        traverseFieldTypeDeclaration(valuesType, path); //path unchanged
        return;
      case "record":
        traverseRecordDefinition(typeObject, path); //path unchanged
        break;
      default:
        throw new IllegalStateException("unhandled type " + typeObject);
    }
  }
}
