/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.backports;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AvroNames {

    private String badSpace; // current namespace under pre-avro-702 logic
    private String correctSpace; //current namespace under correct (post avro-702) implementation
    private final Map<AvroName, Schema> known = new HashMap<>();

    public AvroNames() {
    }

    public String badSpace() {
        return badSpace;
    }

    public String correctSpace() {
        return correctSpace;
    }

    public void badSpace(String space) {
        this.badSpace = space;
    }

    public void correctSpace(String space) {
        this.correctSpace = space;
    }

    public boolean contains(Schema schema) {
        AvroName name = AvroName.of(schema);
        return known.containsKey(name);
    }

    public Schema get(AvroName name) {
        return known.get(name);
    }

    public Schema put(AvroName name, Schema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }
        if (known.containsKey(name)) {
            throw new SchemaParseException("Can't redefine: " + name);
        }
        return known.put(name, schema);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (!Objects.equals(badSpace, correctSpace)) {
            sb.append("wrong space = ").append(badSpace);
            sb.append(", correct space = ").append(correctSpace);
        } else {
            sb.append("space = ").append(badSpace);
        }
        if (!known.isEmpty()) {
            List<AvroName> sorted = new ArrayList<>(known.keySet());
            sorted.sort(AvroName.BY_FULLNAME);
            sb.append(", known = ").append(sorted);
        }
        return sb.toString();
    }
}
