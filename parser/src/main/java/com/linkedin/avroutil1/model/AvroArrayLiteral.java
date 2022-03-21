/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.List;
import java.util.StringJoiner;

public class AvroArrayLiteral extends AvroLiteral {
    private final List<AvroLiteral> value;

    public AvroArrayLiteral(AvroArraySchema schema, CodeLocation codeLocation, List<AvroLiteral> value) {
        super(schema, codeLocation);
        if (!AvroType.ARRAY.equals(schema.type())) {
            throw new IllegalArgumentException("schema " + schema
                    + " is not an array schema but rather a " + schema.type());
        }
        //TODO - validate values vs value schema
        this.value = value;
    }

    public List<AvroLiteral> getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringJoiner csv = new StringJoiner(", ");
        for (AvroLiteral v : value) {
            csv.add(String.valueOf(v));
        }
        return "[" + csv + "]";
    }
}
