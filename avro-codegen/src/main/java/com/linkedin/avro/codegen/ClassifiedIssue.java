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
