/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

public interface SchemaTransformStep {
  /**
   * applies this step to the given schema json
   * @param schemaJson a schema json
   * @return resulting schema json after applying transform to the input
   */
  default String applyToSchema(String schemaJson) {
    return applyToText(schemaJson);
  }

  /**
   * applies this step to a json-serialized avro object
   * @param payloadJson a json serialized avro object instance
   * @return the json serialized object after applying this transform
   */
  default String applyToJsonObject(String payloadJson) {
    return applyToText(payloadJson);
  }

  /**
   * applies this transform to a (presumably unstructured) piece of text
   * @param text some text
   * @return the text, with the transform applied
   */
  default String applyToText(String text) {
    return text; //nop
  }

  /**
   * producers the inverse transform to this one
   * @return the inverse transform
   */
  SchemaTransformStep inverse();
}
