/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroFixedSchema extends AvroNamedSchema {
    private final int size;

    public AvroFixedSchema(CodeLocation codeLocation, String simpleName, String namespace, String doc, int size) {
        super(codeLocation, simpleName, namespace, doc);
        this.size = size;
    }

    @Override
    public AvroType type() {
        return AvroType.FIXED;
    }

    public int getSize() {
        return size;
    }
}
