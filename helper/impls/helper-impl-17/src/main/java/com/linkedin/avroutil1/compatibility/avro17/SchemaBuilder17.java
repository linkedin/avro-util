/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.AbstractSchemaBuilder;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.Map;

public class SchemaBuilder17 extends AbstractSchemaBuilder {

    private Map<String, JsonNode> _props;

    public SchemaBuilder17(AvroAdapter adapter, Schema original) {
        super(adapter, original);
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
                if (_fields != null && !_fields.isEmpty()) {
                    result.setFields(cloneFields(_fields));
                }
                if (_props != null && !_props.isEmpty()) {
                    Avro17Utils.setProps(result, _props);
                }
                break;
            default:
                throw new UnsupportedOperationException("unhandled type " + _type);
        }
        return result;
    }
}
