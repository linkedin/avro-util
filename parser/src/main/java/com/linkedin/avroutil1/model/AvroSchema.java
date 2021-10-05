/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Locale;

/**
 * parent class for all avro types
 */
public abstract class AvroSchema implements LocatedCode {
    protected final CodeLocation codeLocation;

    public AvroSchema(CodeLocation codeLocation) {
        this.codeLocation = codeLocation;
    }

    public abstract AvroType type();

    public abstract AvroLogicalType logicalType();

    @Override
    public CodeLocation getCodeLocation() {
        return codeLocation;
    }

    /**
     * @return true if this schema allows null values/literals
     */
    public boolean isNullable() {
        return false; //as a rule avro schemas do not allow nulls
    }

    @Override
    public String toString() {
        String typeName = type().name().toLowerCase(Locale.ROOT);
        AvroLogicalType logicalType = logicalType();
        if (logicalType == null) {
            return typeName;
        }
        return typeName + " (" + logicalType + ")";
    }
}
