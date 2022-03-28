/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.List;


/**
 * parent class for all avro named types: records, enums and fixed
 */
public abstract class AvroNamedSchema extends AvroSchema {

    protected final AvroName name;
    protected final List<AvroName> aliases; //as fullnames
    protected final String doc;

    public AvroNamedSchema(
        CodeLocation codeLocation,
        AvroName name,
        List<AvroName> aliases,
        String doc,
        JsonPropertiesContainer props
    ) {
        super(codeLocation, props);
        if (name == null) {
            throw new IllegalArgumentException("name cannot be null");
        }
        this.name = name;
        this.aliases = aliases;
        this.doc = doc;
    }

    public AvroName getName() {
        return name;
    }

    public String getSimpleName() {
        return name.getSimpleName();
    }

    public String getNamespace() {
        return name.getNamespace();
    }

    public String getFullName() {
        return name.getFullname();
    }

    public List<AvroName> getAliases() {
        return aliases;
    }

    public String getDoc() {
        return doc;
    }

    @Override
    public String toString() {
        return super.toString() + " " + getFullName();
    }
}
