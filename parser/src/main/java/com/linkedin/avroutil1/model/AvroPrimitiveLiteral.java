/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public abstract class AvroPrimitiveLiteral extends AvroLiteral {

    public AvroPrimitiveLiteral(AvroPrimitiveSchema schema, CodeLocation codeLocation) {
        super(schema, codeLocation);
        if (!primitiveType().equals(schema.type())) {
            throw new IllegalArgumentException("schema " + schema + " is not a "
                    + primitiveType() + " schema but rather a " + schema.type());
        }
    }

    protected abstract AvroType primitiveType();
}
