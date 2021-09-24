/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.CodeLocation;

/**
 * represents something being wrong with an avsc source (usually a file)
 */
public class AvscIssue {
    private final CodeLocation location;
    private final IssueSeverity severity;
    private final String message;
    private final Throwable throwable;

    public AvscIssue(CodeLocation location, IssueSeverity severity, String message, Throwable throwable) {
        this.location = location;
        this.severity = severity;
        this.message = message;
        this.throwable = throwable;
    }

    public CodeLocation getLocation() {
        return location;
    }

    public IssueSeverity getSeverity() {
        return severity;
    }

    public String getMessage() {
        return message;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public String toString() {
        return severity + " @ " + location.getStart() + ": " + message;
    }
}
