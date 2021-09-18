/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.parser.Located;

public class AvscParseResult {
    /**
     * a fatal error preventing the complete parsing of the avsc schema
     */
    private Throwable parseError;
    /**
     * parse context. set on successful completion of parsing
     */
    private AvscParseContext context;

    public void recordError(Throwable error) {
        if (this.context != null) {
            throw new IllegalStateException("parsing already marked as succeeded");
        }
        if (parseError == null) {
            parseError = error;
        } else {
            //we allow recording multiple errors
            parseError.addSuppressed(error);
        }
    }

    public void recordParseComplete(AvscParseContext context) {
        if (parseError != null) {
            throw new IllegalStateException("parsing already marked as failed");
        }
        if (this.context != null) {
            throw new IllegalStateException("parsing already marked as succeeded");
        }
        this.context = context;
    }

    public Throwable getParseError() {
        return parseError;
    }

    public AvroSchema getTopLevelSchema() {
        assertSuccess();
        Located<AvroSchema> topLevelSchema = context.getTopLevelSchema();
        return topLevelSchema == null ? null : topLevelSchema.getValue();
    }

    protected void assertSuccess() {
        if (parseError != null) {
            throw new IllegalStateException("parsing has failed", parseError);
        }
        if (context == null) {
            throw new IllegalStateException("parsing not (yet?) marked complete");
        }
    }

    @Override
    public String toString() {
        if (parseError != null) {
            return "failed with " + parseError.getMessage();
        }
        return "succeeded with " + context.getAllDefinedSchemas().size() + " schemas parsed";
    }
}
