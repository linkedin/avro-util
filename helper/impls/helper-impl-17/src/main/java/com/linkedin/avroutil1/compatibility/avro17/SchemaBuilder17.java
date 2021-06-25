/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.SchemaBuilder;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SchemaBuilder17 implements SchemaBuilder {

    private Schema.Type _type;
    private String _name;
    private String _namespace;
    private String _doc;
    private boolean _isError;
    private List<Schema.Field> _fields;
    private Map<String, JsonNode> _props;

    public SchemaBuilder17(Schema original) {
        _type = original.getType();
        _name = original.getName();
        _namespace = original.getNamespace();
        _doc = original.getDoc();
        _isError = original.isError();
        _fields = original.getFields();
        _props = Avro17Utils.getProps(original);
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
                    Avro17Utils.setProps(result, _props);
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
            FieldBuilder17 fb = new FieldBuilder17(original);
            Schema.Field clone = fb.build();
            clones.add(clone);
        }
        return clones;
    }
}
