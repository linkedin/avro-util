/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import com.linkedin.avroutil1.compatibility.SchemaBuilder;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaBuilder110 implements SchemaBuilder {

    private Schema.Type _type;
    private String _name;
    private String _namespace;
    private String _doc;
    private boolean _isError;
    private List<Schema.Field> _fields;
    private Map<String, Object> _props;

    public SchemaBuilder110(Schema original) {
        _type = original.getType();
        _name = original.getName();
        _namespace = original.getNamespace();
        _doc = original.getDoc();
        _isError = original.isError();
        _fields = original.getFields();
        _props = original.getObjectProps();
    }

    @Override
    public Schema build() {
        if (_type == null) {
            throw new IllegalArgumentException("type not set");
        }
        Schema result;
        //noinspection SwitchStatementWithTooFewBranches
        switch (_type) {
            case RECORD:
                result = Schema.createRecord(_name, _doc, _namespace, _isError);
                result.setFields(cloneFields(_fields));
                if (_props != null && !_props.isEmpty()) {
                    for (Map.Entry<String, Object> entry : _props.entrySet()) {
                        result.addProp(entry.getKey(), entry.getValue());
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("unhandled type " + _type);
        }
        return result;
    }

    /**
     * {@link Schema.Field} has a position ("pos") property that is set when its added to a schema.
     * this means we need to clone fields to add them to another schema
     * @param originals list of fields to clone
     * @return list of cloned fields
     */
    private List<Schema.Field> cloneFields(List<Schema.Field> originals) {
        List<Schema.Field> clones = new ArrayList<>(originals.size());
        for (Schema.Field original : originals) {
            FieldBuilder110 fb = new FieldBuilder110(original);
            Schema.Field clone = fb.build();
            clones.add(clone);
        }
        return clones;
    }
}
