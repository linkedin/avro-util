/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import com.linkedin.avroutil1.compatibility.AbstractSchemaBuilder;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import java.util.HashMap;
import org.apache.avro.Schema;

import java.util.Map;

public class SchemaBuilder110 extends AbstractSchemaBuilder {

    private Map<String, Object> _props;

    public SchemaBuilder110(AvroAdapter adapter, Schema original) {
        super(adapter, original);
        if (original != null) {
            _props = original.getObjectProps();
        } else {
            _props= new HashMap<>(1);
        }
    }

    @Override
    protected Schema buildEnumSchemaInternal() {
        return Schema.createEnum(_name, _doc, _namespace, _symbols, _defaultSymbol);
    }

    @Override
    protected void setPropsInternal(Schema result) {
        if (_props != null && !_props.isEmpty()) {
            for (Map.Entry<String, Object> entry : _props.entrySet()) {
                result.addProp(entry.getKey(), entry.getValue());
            }
        }
    }
}
