/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroIntegerLiteral extends AvroPrimitiveLiteral {
    private final int value;

    public AvroIntegerLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation, int value) {
        super(schema, codeLocation);
        this.value = value;
    }

    @Override
    protected AvroType expectedType() {
        return AvroType.INT;
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }
}
