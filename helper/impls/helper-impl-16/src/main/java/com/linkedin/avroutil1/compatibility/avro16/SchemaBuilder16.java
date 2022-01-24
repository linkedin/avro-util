/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.compatibility.AbstractSchemaBuilder;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import org.apache.avro.Schema;

import java.util.Map;

public class SchemaBuilder16 extends AbstractSchemaBuilder {

    private Map<String, String> _props;

    public SchemaBuilder16(AvroAdapter adapter, Schema original) {
        super(adapter, original);
        _props = original.getProps();
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
                if (_fields != null && !_fields.isEmpty()) {
                    result.setFields(cloneFields(_fields));
                }
                if (_props != null && !_props.isEmpty()) {
                    for (Map.Entry<String, String> entry : _props.entrySet()) {
                        result.addProp(entry.getKey(), entry.getValue());
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("unhandled type " + _type);
        }
        return result;
    }
}
