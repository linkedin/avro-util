/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroDoubleLiteral extends AvroPrimitiveLiteral {
    private final double value;

    public AvroDoubleLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation, double value) {
        super(schema, codeLocation);
        this.value = value;
    }

    @Override
    protected AvroType expectedType() {
        return AvroType.DOUBLE;
    }

    @Override
    public String toString() {
        return value + "D";
    }
}
