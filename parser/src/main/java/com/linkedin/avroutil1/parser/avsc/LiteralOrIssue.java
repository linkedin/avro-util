/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroLiteral;

public class LiteralOrIssue {
    private final AvroLiteral literal;
    private final AvscIssue issue;

    public LiteralOrIssue(AvroLiteral literal) {
        if (literal == null) {
            throw new IllegalArgumentException("literal cannot be null");
        }
        this.literal = literal;
        this.issue = null;
    }

    public LiteralOrIssue(AvscIssue issue) {
        if (issue == null) {
            throw new IllegalArgumentException("issue cannot be null");
        }
        this.literal = null;
        this.issue = issue;
    }

    public AvroLiteral getLiteral() {
        return literal;
    }

    public AvscIssue getIssue() {
        return issue;
    }
}
