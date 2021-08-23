/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Locale;

/**
 * covers all avro primitive and complex built-in types.
 * DOES NOT cover types added in the avro protocol declaration
 * (like "error")
 */
public enum AvroType {
    NULL   (false, true),
    BOOLEAN(false, true),
    INT    (false, true),
    ENUM   (true,  false),
    FLOAT  (false, true),
    LONG   (false, true),
    DOUBLE (false, true),
    STRING (false, true),
    BYTES  (false, true),
    FIXED  (true,  false),
    ARRAY  (false, false),
    MAP    (false, false),
    UNION  (false, false),
    RECORD (true,  false);

    /**
     * whether this is one of avro's named types
     */
    private final boolean named;

    /**
     * whether this is one of avro's primitive types
     */
    private final boolean primitive;

    AvroType(boolean named, boolean primitive) {
        this.named = named;
        this.primitive = primitive;
    }

    public boolean isNamed() {
        return named;
    }

    public boolean isPrimitive() {
        return primitive;
    }

    public static AvroType fromJson(String jsonTypeStr) {
        //todo - optimize to not rely on exception
        try {
            return valueOf(jsonTypeStr.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
