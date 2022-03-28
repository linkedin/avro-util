/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroStringLiteral extends AvroPrimitiveLiteral {
    private final String value;

    public AvroStringLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation, String value) {
        super(schema, codeLocation);
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    protected AvroType primitiveType() {
        return AvroType.STRING;
    }

    @Override
    public String toString() {
        return value;
    }
}
