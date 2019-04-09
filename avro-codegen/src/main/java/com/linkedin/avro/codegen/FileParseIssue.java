package com.linkedin.avro.codegen;

import org.apache.avro.SchemaParseException;

import java.io.File;

class FileParseIssue {
    private final File file;
    private final SchemaParseException exception;
    private ClassifiedIssue classification;

    public FileParseIssue(File file, SchemaParseException exception) {
        this.file = file;
        this.exception = exception;
    }

    public File getFile() {
        return file;
    }

    public SchemaParseException getException() {
        return exception;
    }

    public ClassifiedIssue getClassification() {
        return classification;
    }

    public void setClassification(ClassifiedIssue classification) {
        this.classification = classification;
    }
}
