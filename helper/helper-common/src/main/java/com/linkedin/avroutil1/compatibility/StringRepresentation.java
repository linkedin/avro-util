/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

/**
 * represents the possible ways avro code generation may represent String fields
 * in generated code
 */
public enum StringRepresentation {
    CharSequence, String, Utf8
}
