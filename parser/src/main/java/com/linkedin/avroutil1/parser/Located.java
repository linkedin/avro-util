/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser;

import com.linkedin.avroutil1.model.TextLocation;

import java.util.Objects;

/**
 * represents something that has a location in a body of text
 * @param <T> something. usually an avro something.
 */
public class Located<T> {
    private final T value;
    //TODO - expand into full CodeLocation
    private final TextLocation location;

    public Located(T value, TextLocation location) {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        this.value = value;
        this.location = location;
    }

    public T getValue() {
        return value;
    }

    public TextLocation getLocation() {
        return location;
    }

    @Override
    public String toString() {
        return value + "@" + location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Located<?> located = (Located<?>) o;
        return Objects.equals(value, located.value) && Objects.equals(location, located.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, location);
    }
}
