/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.stream.JsonLocation;

import java.nio.file.Path;

public class JsonStringExtImpl extends JsonValueExtImpl implements JsonStringExt {
    private final String value;

    public JsonStringExtImpl(
            Path source,
            JsonLocation startLocation,
            JsonLocation endLocation,
            String value
    ) {
        super(source, startLocation, endLocation);
        this.value = value;
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public CharSequence getChars() {
        return value;
    }

    @Override
    public ValueType getValueType() {
        return ValueType.STRING;
    }

    @Override
    public String toString() {
        return value;
    }
}
