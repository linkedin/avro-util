/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroEnumLiteral extends AvroLiteral {
    private final String value;

    public AvroEnumLiteral(AvroEnumSchema schema, CodeLocation codeLocation, String value) {
        super(schema, codeLocation);
        if (!AvroType.ENUM.equals(schema.type())) {
            throw new IllegalArgumentException("schema " + schema
                    + " is not an enum schema but rather a " + schema.type());
        }
        if (!schema.getSymbols().contains(value)) {
            throw new IllegalArgumentException("enum " + schema
                    + " does not have a symbol " + value);
        }
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
