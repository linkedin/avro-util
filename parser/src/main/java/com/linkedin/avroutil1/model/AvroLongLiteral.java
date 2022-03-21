/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroLongLiteral extends AvroPrimitiveLiteral {
    private final long value;

    public AvroLongLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation, long value) {
        super(schema, codeLocation);
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    @Override
    protected AvroType primitiveType() {
        return AvroType.LONG;
    }

    @Override
    public String toString() {
        return value + "L";
    }
}
