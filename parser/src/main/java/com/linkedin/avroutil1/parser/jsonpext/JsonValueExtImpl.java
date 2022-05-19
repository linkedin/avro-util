/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import javax.json.stream.JsonLocation;

import java.nio.file.Path;

public abstract class JsonValueExtImpl implements JsonValueExt {
    private final Path source;
    private final JsonLocation startLocation;
    private final JsonLocation endLocation;

    protected JsonValueExtImpl(Path source, JsonLocation startLocation, JsonLocation endLocation) {
        this.source = source;
        this.startLocation = startLocation;
        this.endLocation = endLocation;
    }

    @Override
    public Path getSource() {
        return source;
    }

    @Override
    public JsonLocation getStartLocation() {
        return startLocation;
    }

    @Override
    public JsonLocation getEndLocation() {
        return endLocation;
    }
}
