/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSchemaBuilder implements SchemaBuilder {

    protected final AvroAdapter _adapter;
    protected Schema.Type _type;
    protected String _name;
    protected String _namespace;
    protected String _doc;
    protected boolean _isError;
    protected List<Schema.Field> _fields;

    protected AbstractSchemaBuilder(AvroAdapter _adapter, Schema original) {
        this._adapter = _adapter;
        if (original != null) {
            _type = original.getType();
            _name = original.getName();
            _namespace = original.getNamespace();
            _doc = original.getDoc();
            _isError = original.isError();
            //make a copy of fields so its mutable
            _fields = new ArrayList<>(original.getFields());
        } else {
            _fields = new ArrayList<>(1);
        }
    }

    @Override
    public SchemaBuilder setType(Schema.Type type) {
        checkSchemaType(type);
        _type = type;
        return this;
    }

    @Override
    public SchemaBuilder addField(Schema.Field field) {
        checkNewField(field);
        _fields.add(field);
        return this;
    }

    @Override
    public SchemaBuilder addField(int position, Schema.Field field) {
        checkNewField(field);
        _fields.add(position, field); //will throw IOOB on bad positions
        return this;
    }

    @Override
    public SchemaBuilder removeField(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("argument cannot be null or empty");
        }
        int index = fieldPositionByName(fieldName, false);
        if (index >= 0) {
            _fields.remove(index);
        }
        return this;
    }

    @Override
    public SchemaBuilder removeField(int position) {
        _fields.remove(position); //throws IOOB
        return this;
    }

    /**
     * {@link Schema.Field} has a position ("pos") property that is set when its added to a schema.
     * this means we need to clone fields to add them to another schema
     * @param originals list of fields to clone
     * @return list of cloned fields
     */
    protected List<Schema.Field> cloneFields(List<Schema.Field> originals) {
        List<Schema.Field> clones = new ArrayList<>(originals.size());
        for (Schema.Field original : originals) {
            FieldBuilder fb = _adapter.cloneSchemaField(original);
            Schema.Field clone = fb.build();
            clones.add(clone);
        }
        return clones;
    }

    protected void checkSchemaType(Schema.Type type) {
        if (type == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
    }

    protected void checkNewField(Schema.Field field) {
        if (field == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        int otherIndex = fieldPositionByName(field.name(), false);
        if (otherIndex >= 0) {
            throw new IllegalArgumentException("schema already contains a field called " + field.name());
        }
    }

    protected int fieldPositionByName(String name, boolean caseSensitive) {
        for (int i = 0; i < _fields.size(); i++) {
            Schema.Field candidate = _fields.get(i);
            String cName = candidate.name();
            if (caseSensitive) {
                if (cName.equals(name)) {
                    return i;
                }
            } else if (cName.equalsIgnoreCase(name)){
                return i;
            }
        }
        return -1;
    }
}
