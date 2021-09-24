/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroBooleanLiteral extends AvroPrimitiveLiteral {
    private final boolean value;

    public AvroBooleanLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation, boolean value) {
        super(schema, codeLocation);
        this.value = value;
    }

    @Override
    protected AvroType expectedType() {
        return AvroType.BOOLEAN;
    }

    @Override
    public String toString() {
        return Boolean.toString(value);
    }
}
