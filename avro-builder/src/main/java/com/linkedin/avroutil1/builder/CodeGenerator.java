/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

/**
 * represents which code generator to use for initial generation of specific record classes
 */
public enum CodeGenerator {
  VANILLA,
  AVRO_UTIL
}
