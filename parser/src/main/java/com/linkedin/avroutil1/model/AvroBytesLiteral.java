/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroBytesLiteral extends AvroPrimitiveLiteral {
    private final byte[] value;

    public AvroBytesLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation, byte[] value) {
        super(schema, codeLocation);
        this.value = value;
    }

    @Override
    protected AvroType expectedType() {
        return AvroType.BYTES;
    }

    @Override
    public String toString() {
        //TODO - hex encode
        return value.length + " bytes";
    }
}
