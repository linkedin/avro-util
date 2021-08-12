/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.stream.JsonLocation;

import java.nio.file.Path;

public class JsonTrueExtImpl extends JsonValueExtImpl implements JsonValueExt {
    public JsonTrueExtImpl(Path source, JsonLocation startLocation, JsonLocation endLocation) {
        super(source, startLocation, endLocation);
    }

    @Override
    public ValueType getValueType() {
        return ValueType.TRUE;
    }

    @Override
    public String toString() {
        return "true";
    }
}
