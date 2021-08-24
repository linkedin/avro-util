/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

public class AvroRecordSchema extends AvroNamedSchema {

    public AvroRecordSchema(String simpleName, String namespace, String doc) {
        super(simpleName, namespace, doc);
    }

    @Override
    public AvroType type() {
        return AvroType.RECORD;
    }
}
