/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.CodeLocation;
import com.linkedin.avroutil1.model.LocatedCode;
import com.linkedin.avroutil1.model.SchemaOrRef;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvscParseResult {
    /**
     * a fatal error preventing the complete parsing of the avsc schema.
     * this being null does not mean other, less-fatal issues might not
     * exist in {@link #context}
     */
    private Throwable parseError;
    /**
     * parse context. set on successful completion of parsing
     */
    private AvscFileParseContext context;

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

    public void recordParseComplete(AvscFileParseContext context) {
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
        return context.getTopLevelSchema();
    }

    public Map<String, AvroSchema> getDefinedNamedSchemas() {
        assertSuccess();
        return context.getDefinedNamedSchemas();
    }

    public List<SchemaOrRef> getExternalReferences() {
        assertSuccess();
        return context.getExternalReferences();
    }

    public List<AvscIssue> getIssues() {
        return context.getIssues();
    }

    public List<AvscIssue> getIssues(LocatedCode relatedTo) {
        CodeLocation span = relatedTo.getCodeLocation();
        return getIssues().stream().filter(issue -> issue.getLocation().overlaps(span)).collect(Collectors.toList());
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
        StringBuilder sb = new StringBuilder();
        sb.append("succeeded with ").append(context.getAllDefinedSchemas().size()).append(" schemas parsed");
        List<AvscIssue> issues = context.getIssues();
        if (!issues.isEmpty()) {
            sb.append(" and ").append(issues.size()).append(" issues");
        }
        List<SchemaOrRef> unresolved = context.getExternalReferences();
        if (!unresolved.isEmpty()) {
            int total = unresolved.size();
            sb.append(" and ")
                .append(total)
                .append(" unresolved references to ")
                .append(unresolved.size())
                .append(" FQCNs");
        }
        return sb.toString();
    }
}
