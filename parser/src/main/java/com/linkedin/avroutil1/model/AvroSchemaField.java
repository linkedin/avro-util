/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * field in an {@link AvroSchema}
 */
public class AvroSchemaField implements LocatedCode {
    private final CodeLocation codeLocation;
    private final String name;
    private final String doc;
    /**
     * this field's schema (which is either declared inline or a reference)
     */
    private final SchemaOrRef schema;
    /**
     * the "parent" record schema of which this field is a part of.
     * once assigned it cannot be changed.
     */
    private AvroRecordSchema parentSchema;
    /**
     * this field's zero-based position among the fields of its
     * parent schema. set once {@link #parentSchema} is assigned
     */
    private int position = -1;

    public AvroSchemaField(CodeLocation codeLocation, String name, String doc, SchemaOrRef schema) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name cannot be null or empty");
        }
        if (schema == null) {
            throw new IllegalArgumentException("schema for " + name + " cannot be null");
        }
        this.codeLocation = codeLocation;
        this.name = name;
        this.doc = doc;
        this.schema = schema;
    }

    @Override
    public CodeLocation getCodeLocation() {
        return codeLocation;
    }

    public String getName() {
        return name;
    }

    public String getDoc() {
        return doc;
    }

    public int getPosition() {
        return position;
    }

    /**
     * @return the schema for this field (either defined inline or referenced from elsewhere)
     * @throws IllegalStateException if this field refers to an unresolved schema
     */
    public AvroSchema getSchema() {
        return schema.getSchema();
    }

    /**
     * @return this field's schema (which may be a reference)
     */
    public SchemaOrRef getSchemaOrRef() {
        return schema;
    }

    void assignTo(AvroRecordSchema recordSchema, int position) {
        if (recordSchema == null) {
            throw new IllegalArgumentException("cant assign null recordSchema to field " + getName());
        }
        if (position < 0) {
            throw new IllegalArgumentException("cant assign negative position " + position + " to field " + getName());
        }
        if (this.parentSchema != null) {
            throw new IllegalStateException("field " + getName() + " is already assigned to record schema "
                    + parentSchema.getFullName() + " and cannot be re-assigned to " + recordSchema.getFullName());
        }
        this.parentSchema = recordSchema;
        this.position = position;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (position >= 0) {
            sb.append(position).append(": ");
        }
        sb.append(schema.toString()).append(" ").append(name);
        return sb.toString();
    }
}
