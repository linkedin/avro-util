/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.stream.JsonLocation;

import java.nio.file.Path;

public abstract class BuilderWithLocations<T extends JsonValueExt> {

    protected Path source;
    protected JsonLocation startLocation;
    protected JsonLocation endLocation;

    public Path getSource() {
        return source;
    }

    public void setSource(Path source) {
        this.source = source;
    }

    public JsonLocation getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(JsonLocation startLocation) {
        this.startLocation = startLocation;
    }

    public JsonLocation getEndLocation() {
        return endLocation;
    }

    public void setEndLocation(JsonLocation endLocation) {
        this.endLocation = endLocation;
    }

    public abstract T build();
}
