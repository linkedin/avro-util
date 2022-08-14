/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * A map schema. Map keys in avro are always "assumed to be strings"
 */
public class AvroMapSchema extends AvroCollectionSchema {

    public AvroMapSchema(CodeLocation codeLocation, SchemaOrRef valueSchema, JsonPropertiesContainer props) {
        super(codeLocation, valueSchema, props);
    }

    @Override
    public AvroType type() {
        return AvroType.MAP;
    }

    @Override
    public String toString() {
        return "Map<string, " + getValueSchema() + ">";
    }
}
