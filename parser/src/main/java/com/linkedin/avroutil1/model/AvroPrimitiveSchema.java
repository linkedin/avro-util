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
    /**
     * type used to represent strings in java generated classes.
     * only valid for string schemas
     */
    private final AvroJavaStringRepresentation javaStringRepresentation;
    /**
     * BYTES with logicalType DECIMAL represent an arbitrary-precision
     * signed decimal number of the form unscaled × 10^-scale.
     * the bytes contain the two's-complement representation of the unscaled integer,
     * while the scale is fixed and specified using this attribute, which defaults
     * to 0 if not specified and must non-negative
     */
    private final int scale;
    /**
     * BYTES with logicalType DECIMAL represent an arbitrary-precision
     * signed decimal number of the form unscaled × 10^-scale.
     * the bytes contain the two's-complement representation of the unscaled integer,
     * and this value (which is required) represents the maximum number of
     * (decimal, base 10) digits stored in this type.
     * value must be greater than or equal to scale. required.
     */
    private final int precision;

    public AvroPrimitiveSchema(
            CodeLocation codeLocation,
            AvroType type,
            AvroLogicalType logicalType,
            AvroJavaStringRepresentation javaStringRepresentation,
            int scale,
            int precision
    ) {
        super(codeLocation);
        if (!type.isPrimitive()) {
            throw new IllegalArgumentException(type + " is not a primitive type");
        }
        if (logicalType != null) {
            if (!logicalType.getParentTypes().contains(type)) {
                throw new IllegalArgumentException(type() + " at " + codeLocation + " cannot have a logical type of "
                        + logicalType + " (which can only be a logical type of " + logicalType.getParentTypes() + ")");
            }
        }
        if (logicalType != AvroLogicalType.DECIMAL) {
            if (scale != 0) {
                throw new IllegalArgumentException(type() + " at " + codeLocation + " cannot have scale specified ("
                        + scale + ") without having an appropriate logicalType");
            }
            if (precision != 0) {
                throw new IllegalArgumentException(type() + " at " + codeLocation + " cannot have precision specified ("
                        + scale + ") without having an appropriate logicalType");
            }
        } else {
            if (scale > precision) {
                throw new IllegalArgumentException(type() + " at " + codeLocation + " has scale ("
                        + scale + ") > precision (" + precision + ")");
            }
        }
        if (javaStringRepresentation != null && type != AvroType.STRING) {
            throw new IllegalArgumentException("cant set a javaStringRepresentation (" + javaStringRepresentation
                    + ") for type " + type + " as it is not a string");
        }
        this.type = type;
        this.logicalType = logicalType;
        this.javaStringRepresentation = javaStringRepresentation;
        this.scale = scale;
        this.precision = precision;
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
        if (type != AvroType.STRING) {
            throw new IllegalStateException("not a string schema");
        }
        return javaStringRepresentation;
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public boolean isNullable() {
        return type == AvroType.NULL;
    }

    @Override
    public String toString() {
        String superValue = super.toString();
        if (javaStringRepresentation == null) {
            return superValue;
        }
        return superValue + " (javaType=" + javaStringRepresentation + ")";
    }

    public static AvroPrimitiveSchema forType(
            CodeLocation codeLocation,
            AvroType type,
            AvroLogicalType logicalType,
            AvroJavaStringRepresentation javaStringRepresentation,
            int scale,
            int precision
    ) {
        return new AvroPrimitiveSchema(
                codeLocation,
                type,
                logicalType,
                javaStringRepresentation,
                scale,
                precision
        );
    }
}
