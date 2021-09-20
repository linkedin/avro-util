/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * covers all avro primitive and complex built-in types.
 * DOES NOT cover types added in the avro protocol declaration
 * (like "error")
 */
public enum AvroType {
    NULL   (false, true,  false),
    BOOLEAN(false, true,  false),
    INT    (false, true,  false),
    ENUM   (true,  false, false),
    FLOAT  (false, true,  false),
    LONG   (false, true,  false),
    DOUBLE (false, true,  false),
    STRING (false, true,  false),
    BYTES  (false, true,  false),
    FIXED  (true,  false, false),
    ARRAY  (false, false, true),
    MAP    (false, false, true),
    UNION  (false, false, false),
    RECORD (true,  false, false);

    private static final List<AvroType> PRIMITIVES;

    static {
        List<AvroType> temp = new ArrayList<>();
        for (AvroType type : values()) {
            if (type.isPrimitive()) {
                temp.add(type);
            }
        }
        PRIMITIVES = Collections.unmodifiableList(temp);
    }

    /**
     * whether this is one of avro's named types
     */
    private final boolean named;

    /**
     * whether this is one of avro's primitive types
     */
    private final boolean primitive;

    /**
     * whether this is one of avro's collection types
     */
    private final boolean collection;

    AvroType(boolean named, boolean primitive, boolean collection) {
        if ((named && (primitive || collection)) || (!named && primitive && collection)) {
            throw new IllegalArgumentException("illegal combination named="
                    + named + ", primitive=" + primitive + ", collection=" + collection);
        }
        this.named = named;
        this.primitive = primitive;
        this.collection = collection;
    }

    public boolean isNamed() {
        return named;
    }

    public boolean isPrimitive() {
        return primitive;
    }

    public boolean isCollection() {
        return collection;
    }

    public static AvroType fromJson(String jsonTypeStr) {
        //todo - optimize to not rely on exception
        try {
            return valueOf(jsonTypeStr.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    public static List<AvroType> primitives() {
        return PRIMITIVES;
    }
}
