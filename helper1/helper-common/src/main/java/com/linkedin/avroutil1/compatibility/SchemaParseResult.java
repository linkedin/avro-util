/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

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
    //the effective configuration of the parser used (may depend on what the user requested and what the runtime avro supports)
    private final SchemaParseConfiguration configUsed;

    public SchemaParseResult(Schema mainSchema, Map<String, Schema> allSchemas, SchemaParseConfiguration configUsed) {
        this.mainSchema = mainSchema;
        this.allSchemas = allSchemas;
        this.configUsed = configUsed;
    }

    public Schema getMainSchema() {
        return mainSchema;
    }

    public Map<String, Schema> getAllSchemas() {
        return allSchemas;
    }

    public SchemaParseConfiguration getConfigUsed() {
        return configUsed;
    }
}
