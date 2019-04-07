/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import org.apache.avro.Schema;

import java.util.Map;

/**
 * result of parsing an avsc file
 */
public class SchemaParseResult {
    //top-level schema defined in file
    private final Schema mainSchema;
    //all schemas defined the file (top-level one included)
    private final Map<String, Schema> allSchemas;

    public SchemaParseResult(Schema mainSchema, Map<String, Schema> allSchemas) {
        this.mainSchema = mainSchema;
        this.allSchemas = allSchemas;
    }

    public Schema getMainSchema() {
        return mainSchema;
    }

    public Map<String, Schema> getAllSchemas() {
        return allSchemas;
    }
}
