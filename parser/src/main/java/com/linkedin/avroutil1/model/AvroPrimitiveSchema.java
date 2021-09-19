/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * an avro "primitive" schema (not a named/collection/union)
 */
public class AvroPrimitiveSchema extends AvroSchema {
    private final AvroType type;

    public AvroPrimitiveSchema(CodeLocation codeLocation, AvroType type) {
        super(codeLocation);
        if (!type.isPrimitive()) {
            throw new IllegalArgumentException(type + " is not a primitive type");
        }
        this.type = type;
    }

    @Override
    public AvroType type() {
        return type;
    }

    public static AvroPrimitiveSchema forType(CodeLocation codeLocation, AvroType type) {
        return new AvroPrimitiveSchema(codeLocation, type);
    }
}
