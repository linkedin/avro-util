/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * parent class for avro collection types - arrays and maps
 */
public abstract class AvroCollectionSchema extends AvroSchema {
    /**
     * this collection's value schema (for array items or map values).
     * either defined inline or a reference to a (named) schema defined elsewhere
     */
    private final SchemaOrRef valueSchema;

    protected AvroCollectionSchema(CodeLocation codeLocation, SchemaOrRef valueSchema, JsonPropertiesContainer props) {
        super(codeLocation, props);
        if (valueSchema == null) {
            throw new IllegalArgumentException("value schema cannot be null");
        }
        this.valueSchema = valueSchema;
    }

    public SchemaOrRef getValueSchemaOrRef() {
        return valueSchema;
    }

    /**
     * @return the schema for this collection's values (either defined inline or referenced from elsewhere)
     * @throws IllegalStateException if this collection refers to an unresolved schema
     */
    public AvroSchema getValueSchema() {
        return valueSchema.getSchema();
    }

    @Override
    public AvroLogicalType logicalType() {
        return null; //collections can have no logical types
    }
}
