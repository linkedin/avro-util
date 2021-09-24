/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * represents a concrete instance (value) belonging to some (concrete) schema.
 * used (for example) to represent field/schema default values
 */
public abstract class AvroLiteral implements LocatedCode {
    protected final AvroSchema schema;
    protected final CodeLocation codeLocation;

    public AvroLiteral(AvroSchema schema, CodeLocation codeLocation) {
        if (schema == null) {
            throw new IllegalArgumentException("schema cannot be null");
        }
        //literals should always belong to a concrete schema
        if (schema.type() == AvroType.UNION) {
            throw new IllegalArgumentException("literal cannot be of a union schema");
        }
        this.schema = schema;
        this.codeLocation = codeLocation;
    }

    public AvroType type() {
        return schema.type();
    }

    @Override
    public CodeLocation getCodeLocation() {
        return codeLocation;
    }
}
