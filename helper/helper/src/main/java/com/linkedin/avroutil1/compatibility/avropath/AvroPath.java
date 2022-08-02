/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avropath;

import java.util.ArrayList;
import java.util.List;

public class AvroPath {
    private final List<PathElement> elements;

    public AvroPath() {
        elements = new ArrayList<>(1);
    }

    private AvroPath(List<PathElement> elements) {
        this.elements = elements;
    }

    public void appendPath(PathElement element) {
        if (element == null) {
            throw new IllegalArgumentException("element cannot be null");
        }
        //TODO - input validation vs current path "edge"
        elements.add(element);
    }

    public PathElement pop() {
        if (elements.isEmpty()) {
            throw new IllegalStateException("cant pop from an empty AvroPath");
        }
        return elements.remove(elements.size() - 1);
    }

    public AvroPath copy() {
        return new AvroPath(new ArrayList<>(elements)); //individual elements presumed immutable
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (PathElement element : elements) {
            sb.append(element.toString());
        }
        return sb.toString();
    }
}
