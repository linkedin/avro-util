/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroFloatLiteral extends AvroPrimitiveLiteral {
    private final float value;

    public AvroFloatLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation, float value) {
        super(schema, codeLocation);
        this.value = value;
    }

    public float getValue() {
        return value;
    }

    @Override
    protected AvroType primitiveType() {
        return AvroType.FLOAT;
    }

    @Override
    public String toString() {
        return value + "F";
    }

    @Override
    public boolean equals(Object literal) {
        return literal instanceof AvroFloatLiteral && this.value == ((AvroFloatLiteral) literal).getValue();
    }
}
