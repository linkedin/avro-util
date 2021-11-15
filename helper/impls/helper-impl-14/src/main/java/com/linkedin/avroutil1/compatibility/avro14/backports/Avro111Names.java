/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14.backports;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Avro111Names extends LinkedHashMap<Avro111Name, Schema> {
    private static final long serialVersionUID = 1L;
    private static final Map<String, Schema.Type> PRIMITIVES = new HashMap<>();
    static {
        PRIMITIVES.put("string", org.apache.avro.Schema.Type.STRING);
        PRIMITIVES.put("bytes", org.apache.avro.Schema.Type.BYTES);
        PRIMITIVES.put("int", org.apache.avro.Schema.Type.INT);
        PRIMITIVES.put("long", org.apache.avro.Schema.Type.LONG);
        PRIMITIVES.put("float", org.apache.avro.Schema.Type.FLOAT);
        PRIMITIVES.put("double", org.apache.avro.Schema.Type.DOUBLE);
        PRIMITIVES.put("boolean", org.apache.avro.Schema.Type.BOOLEAN);
        PRIMITIVES.put("null", org.apache.avro.Schema.Type.NULL);
    }

    private String space; // default namespace

    public Avro111Names() {
    }

    public Avro111Names(String space) {
        this.space = space;
    }

    public String space() {
        return space;
    }

    public void space(String space) {
        this.space = space;
    }

    public Schema get(String o) {
        Schema.Type primitive = PRIMITIVES.get(o);
        if (primitive != null) {
            return Schema.create(primitive);
        }
        Avro111Name name = new Avro111Name(o, space);
        if (!containsKey(name)) {
            // if not in default try anonymous
            name = new Avro111Name(o, "");
        }
        return super.get(name);
    }

    public boolean contains(Schema schema) {
        return get(new Avro111Name(schema.getFullName(), null)) != null;
    }

    public void add(Schema schema) {
        put(new Avro111Name(schema.getFullName(), null), schema);
    }

    @Override
    public Schema put(Avro111Name name, Schema schema) {
        if (containsKey(name)) {
            throw new SchemaParseException("Can't redefine: " + name);
        }
        return super.put(name, schema);
    }
}
