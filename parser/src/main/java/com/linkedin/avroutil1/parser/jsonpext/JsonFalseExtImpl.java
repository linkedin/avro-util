/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import javax.json.stream.JsonLocation;

import java.nio.file.Path;

public class JsonFalseExtImpl extends JsonValueExtImpl implements JsonValueExt {
    public JsonFalseExtImpl(Path source, JsonLocation startLocation, JsonLocation endLocation) {
        super(source, startLocation, endLocation);
    }

    @Override
    public ValueType getValueType() {
        return ValueType.FALSE;
    }

    @Override
    public String toString() {
        return "false";
    }
}
