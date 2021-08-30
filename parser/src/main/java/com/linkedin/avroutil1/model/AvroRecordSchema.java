/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AvroRecordSchema extends AvroNamedSchema {
    private List<AvroSchemaField> fields;

    public AvroRecordSchema(String simpleName, String namespace, String doc) {
        super(simpleName, namespace, doc);
    }

    public void setFields(List<AvroSchemaField> fields) {
        if (fields == null) {
            throw new IllegalArgumentException("fields cannot be null");
        }
        if (this.fields != null) {
            throw new IllegalStateException("fields for record " + getFullName() + " have already been set");
        }
        //TODO - check for dup fields (+aliases?), also same-name-different-case
        List<AvroSchemaField> copy = new ArrayList<>(fields); //much defensive
        int counter = 0;
        for (AvroSchemaField field : copy) {
            int position = counter++;
            field.assignTo(this, position);
        }
        this.fields = Collections.unmodifiableList(copy);
    }

    public List<AvroSchemaField> getFields() {
        return fields;
    }

    @Override
    public AvroType type() {
        return AvroType.RECORD;
    }
}
