/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * An array schema
 */
public class AvroArraySchema extends AvroCollectionSchema {

    public AvroArraySchema(CodeLocation codeLocation, SchemaOrRef valueSchema) {
        super(codeLocation, valueSchema);
    }

    @Override
    public AvroType type() {
        return AvroType.ARRAY;
    }

    @Override
    public String toString() {
        return getValueSchema() + "[]";
    }
}
