/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Set;

/**
 * field in an {@link AvroSchema}
 */
public class AvroSchemaField implements LocatedCode, JsonPropertiesContainer {
    /**
     * location of this field declaration in source file. starts at the beginning of the field
     * name and ends at the end of the field declaration/reference.
     */
    private final CodeLocation codeLocation;
    /**
     * field name
     */
    private final String name;
    /**
     * field doc
     */
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
    /**
     * this field's default value (if any).
     */
    private AvroLiteral defaultValue;
    /**
     * any extra properties defined on this field beyond the ones in the core avro specification
     */
    private JsonPropertiesContainer props;

    public AvroSchemaField(
            CodeLocation codeLocation,
            String name,
            String doc,
            SchemaOrRef schema,
            AvroLiteral defaultValue,
            JsonPropertiesContainer extraProps
    ) {
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
        this.defaultValue = defaultValue;
        this.props = extraProps;
    }

    @Override
    public CodeLocation getCodeLocation() {
        return codeLocation;
    }

    public String getName() {
        return name;
    }

    public boolean hasDoc() {
        return doc != null; //empty string counts
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

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    public AvroLiteral getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(AvroLiteral defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public Set<String> propertyNames() {
        return props.propertyNames();
    }

    @Override
    public Object getPropertyAsObject(String key) {
        return props.getPropertyAsObject(key);
    }

    @Override
    public String getPropertyAsJsonLiteral(String key) {
        return props.getPropertyAsJsonLiteral(key);
    }

    public JsonPropertiesContainer getAllProps() {
        return props;
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
