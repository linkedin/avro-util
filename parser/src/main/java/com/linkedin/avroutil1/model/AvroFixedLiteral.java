/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroFixedLiteral extends AvroLiteral {
    private final byte[] value;

    public AvroFixedLiteral(AvroFixedSchema schema, CodeLocation codeLocation, byte[] value) {
        super(schema, codeLocation);
        if (!AvroType.FIXED.equals(schema.type())) {
            throw new IllegalArgumentException("schema " + schema
                    + " is not a fixed schema but rather a " + schema.type());
        }
        if (schema.getSize() != value.length) {
            throw new IllegalArgumentException("value " + value //TODO - encode to something readable
                    + " does not match fixed schema size of " + schema.getSize());
        }
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        //TODO - hex encode
        return value.length + " bytes";
    }

    @Override
    public boolean equals(Object literal) {
        return literal instanceof AvroFixedLiteral && this.value == ((AvroFixedLiteral) literal).getValue();
    }
}
