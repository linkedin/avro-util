/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * an avro record schema
 */
public class AvroRecordSchema extends AvroNamedSchema {
    /**
     * record fields. immutable once set
     */
    private List<AvroSchemaField> fields;

    public AvroRecordSchema(CodeLocation codeLocation, String simpleName, String namespace, String doc) {
        super(codeLocation, simpleName, namespace, doc);
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
        if (fields == null) {
            throw new IllegalStateException("fields for record " + getFullName() + " have never been set");
        }
        return fields;
    }

    public AvroSchemaField getField(String byName) {
        if (fields == null) {
            throw new IllegalStateException("fields for record " + getFullName() + " have never been set");
        }
        for (AvroSchemaField field : fields) {
            if (field.getName().equals(byName)) {
                return field;
            }
        }
        return null;
    }

    @Override
    public AvroType type() {
        return AvroType.RECORD;
    }
}
