/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * an avro "primitive" schema (not a named/collection/union)
 */
public class AvroPrimitiveSchema extends AvroSchema {
    private final AvroType type;
    private final AvroLogicalType logicalType;
    private final AvroJavaStringRepresentation javaStringRepresentation; //only valid for string schemas

    public AvroPrimitiveSchema(
            CodeLocation codeLocation,
            AvroType type,
            AvroLogicalType logicalType,
            AvroJavaStringRepresentation javaStringRepresentation
    ) {
        super(codeLocation);
        if (!type.isPrimitive()) {
            throw new IllegalArgumentException(type + " is not a primitive type");
        }
        if (logicalType != null && !logicalType.getParentTypes().contains(type)) {
            throw new IllegalArgumentException(type() + " at " + codeLocation + " cannot have a logical type of "
                    + logicalType + " (which can only be a logical type of " + logicalType.getParentTypes() + ")");
        }
        if (javaStringRepresentation != null && !AvroType.STRING.equals(type)) {
            throw new IllegalArgumentException("cant set a javaStringRepresentation (" + javaStringRepresentation
                    + ") for type " + type + " as it is not a string");
        }
        this.type = type;
        this.logicalType = logicalType;
        this.javaStringRepresentation = javaStringRepresentation;
    }

    @Override
    public AvroType type() {
        return type;
    }

    @Override
    public AvroLogicalType logicalType() {
        return logicalType;
    }

    public AvroJavaStringRepresentation getJavaStringRepresentation() {
        if (!AvroType.STRING.equals(type())) {
            throw new IllegalStateException("not a string schema");
        }
        return javaStringRepresentation;
    }

    @Override
    public boolean isNullable() {
        return type == AvroType.NULL;
    }

    @Override
    public String toString() {
        if (javaStringRepresentation == null || !AvroType.STRING.equals(type)) {
            return super.toString();
        }
        //reflect avro string type for string schemas
        String typeName = javaStringRepresentation.getJsonValue();
        AvroLogicalType logicalType = logicalType();
        if (logicalType == null) {
            return typeName;
        }
        return typeName + " (" + logicalType + ")";
    }

    public static AvroPrimitiveSchema forType(
            CodeLocation codeLocation,
            AvroType type,
            AvroLogicalType logicalType,
            AvroJavaStringRepresentation javaStringRepresentation
    ) {
        return new AvroPrimitiveSchema(codeLocation, type, logicalType, javaStringRepresentation);
    }
}
