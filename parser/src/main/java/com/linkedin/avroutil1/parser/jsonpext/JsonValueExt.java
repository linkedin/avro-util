/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import javax.json.JsonValue;
import javax.json.stream.JsonLocation;

import java.nio.file.Path;

/**
 * extends {@link javax.json.JsonValue} with its start (inclusive)
 * and end (inclusive?) locations in a file.
 */
public interface JsonValueExt extends JsonValue {
    Path getSource();
    JsonLocation getStartLocation();
    JsonLocation getEndLocation();
}
