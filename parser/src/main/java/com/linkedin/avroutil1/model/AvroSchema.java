/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Locale;
import java.util.Set;

/**
 * parent class for all avro types
 */
public abstract class AvroSchema implements LocatedCode, JsonPropertiesContainer {
    protected final CodeLocation codeLocation;
    /**
     * any extra properties defined on this schema beyond the ones in the core avro specification
     */
    protected JsonPropertiesContainer props;

    protected AvroSchema(CodeLocation codeLocation, JsonPropertiesContainer props) {
        this.codeLocation = codeLocation;
        this.props = props;
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
    public Set<String> propertyNames() {
        return props.propertyNames();
    }

    @Override
    public Object getPropertyAsObject(String key) {
        return props.getPropertyAsObject(key);
    }

    @Override
    public String getPropertyAsJsonLiteral(String key) {
        return props.getPropertyAsJsonLiteral(key);
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
