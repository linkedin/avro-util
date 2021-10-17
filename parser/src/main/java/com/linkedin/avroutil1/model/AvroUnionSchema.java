/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

/**
 * an avro union schema
 */
public class AvroUnionSchema extends AvroSchema {
    /**
     * union elements. immutable once set
     */
    private List<SchemaOrRef> types;

    public AvroUnionSchema(CodeLocation codeLocation) {
        super(codeLocation, EMPTY); //cant put extra props on a union
    }

    @Override
    public AvroType type() {
        return AvroType.UNION;
    }

    @Override
    public AvroLogicalType logicalType() {
        return null; //unions can have no logical types
    }

    public List<SchemaOrRef> getTypes() {
        return types;
    }

    public void setTypes(List<SchemaOrRef> types) {
        if (types == null) {
            throw new IllegalArgumentException("types cannot be null");
        }
        if (this.types != null) {
            //TODO - print this union's code location for extra clarity
            throw new IllegalStateException("fields for union have already been set");
        }
        //TODO - check for dup types (+aliases?)
        List<SchemaOrRef> copy = new ArrayList<>(types); //much defensive
        this.types = Collections.unmodifiableList(copy);
    }

    @Override
    public boolean isNullable() {
        if (types == null) {
            throw new IllegalArgumentException("types cannot be null");
        }
        //unions allow null if any of the branches do (in theory only a single branch should)
        for (SchemaOrRef branch : types) {
            if (branch.getSchema().isNullable()) {
                return true;
            }
        }
        return false;
    }


    @Override
    public String toString() {
        if (types == null) {
            return "union in progress?";
        }
        StringJoiner csv = new StringJoiner(", ");
        for (SchemaOrRef branch : types) {
            csv.add(branch.toString());
        }
        return "[" + csv + "]";
    }
}
