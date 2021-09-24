/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroNullLiteral extends AvroPrimitiveLiteral {

    public AvroNullLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation) {
        super(schema, codeLocation);
    }

    @Override
    protected AvroType expectedType() {
        return AvroType.NULL;
    }

    @Override
    public String toString() {
        return "null";
    }
}
