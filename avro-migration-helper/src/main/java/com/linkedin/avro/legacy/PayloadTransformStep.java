/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

public interface PayloadTransformStep {
  String applyToJsonPayload(String jsonPayload);
  PayloadTransformStep inverse();
}
