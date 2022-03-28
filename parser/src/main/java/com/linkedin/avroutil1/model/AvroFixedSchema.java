/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.List;


public class AvroFixedSchema extends AvroNamedSchema {
    private final int size;
    private final AvroLogicalType logicalType;

    public AvroFixedSchema(
            CodeLocation codeLocation,
            AvroName name,
            List<AvroName> aliases,
            String doc,
            int size,
            AvroLogicalType logicalType,
            JsonPropertiesContainer props
    ) {
        super(codeLocation, name, aliases, doc, props);
        if (logicalType != null && !logicalType.getParentTypes().contains(type())) {
            throw new IllegalArgumentException(type() + " " + getSimpleName() + " at " + codeLocation
                    + " cannot have a logical type of " + logicalType + " (which can only be a logical type of "
                    + logicalType.getParentTypes() + ")");
        }
        this.size = size;
        this.logicalType = logicalType;
    }

    @Override
    public AvroType type() {
        return AvroType.FIXED;
    }

    @Override
    public AvroLogicalType logicalType() {
        return logicalType;
    }

    public int getSize() {
        return size;
    }
}
