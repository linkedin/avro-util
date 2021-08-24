/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Locale;

/**
 * parent class for all avro named types: records, enums and fixed
 */
public abstract class AvroNamedSchema extends AvroSchema {

    protected final String simpleName;
    protected final String namespace;
    protected final String doc;

    public AvroNamedSchema(String simpleName, String namespace, String doc) {
        this.simpleName = simpleName;
        this.namespace = namespace;
        this.doc = doc;
    }

    public String getFullName() {
        if (namespace.isEmpty()) {
            return simpleName;
        }
        return namespace + "." + simpleName;
    }

    @Override
    public String toString() {
        return type().name().toLowerCase(Locale.ROOT) + " " + getFullName();
    }
}
