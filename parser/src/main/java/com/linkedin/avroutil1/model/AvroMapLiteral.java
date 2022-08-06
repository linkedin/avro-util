/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Map;
import java.util.StringJoiner;


public class AvroMapLiteral extends AvroLiteral {
    private final Map<String, AvroLiteral> value;

    public AvroMapLiteral(AvroMapSchema schema, CodeLocation codeLocation, Map<String, AvroLiteral> value) {
        super(schema, codeLocation);
        if (!AvroType.MAP.equals(schema.type())) {
            throw new IllegalArgumentException("schema " + schema
                    + " is not a map schema but rather a " + schema.type());
        }
        //TODO - validate values vs value schema
        this.value = value;
    }

    public Map<String, AvroLiteral> getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringJoiner csv = new StringJoiner(", ");
        for (Map.Entry<String, AvroLiteral> entry : value.entrySet()) {
            csv.add(entry.getKey() + ": " + entry.getValue());
        }
        return "[" + csv + "]";
    }
}
