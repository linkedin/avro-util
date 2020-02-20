/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.codegen;

class ClassifiedIssue {
    private final IssueType type;
    private final String fqcn;

    public ClassifiedIssue(IssueType type, String fqcn) {
        this.type = type;
        this.fqcn = fqcn;
    }

    public IssueType getType() {
        return type;
    }

    public String getFqcn() {
        return fqcn;
    }
}
