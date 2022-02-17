/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * parent class for all avro named types: records, enums and fixed
 */
public abstract class AvroNamedSchema extends AvroSchema {

    protected final String simpleName;
    protected final String namespace;
    protected final String doc;

    public AvroNamedSchema(CodeLocation codeLocation, String simpleName, String namespace, String doc, JsonPropertiesContainer props) {
        super(codeLocation, props);
        if (simpleName == null || simpleName.isEmpty()) {
            throw new IllegalArgumentException("simpleName cannot be null or empty");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("namespace for " + simpleName + " cannot be null"); //can be empty
        }
        this.simpleName = simpleName;
        this.namespace = namespace;
        this.doc = doc;
    }

    public String getSimpleName() {
        return simpleName;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getFullName() {
        if (namespace.isEmpty()) {
            return simpleName;
        }
        return namespace + "." + simpleName;
    }

    public String getDoc() {
        return doc;
    }

    @Override
    public String toString() {
        return super.toString() + " " + getFullName();
    }
}
