/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.compatibility.AbstractSchemaBuilder;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import java.util.HashMap;
import org.apache.avro.Schema;

import java.util.Map;

public class SchemaBuilder16 extends AbstractSchemaBuilder {

    private Map<String, String> _props;

    public SchemaBuilder16(AvroAdapter adapter, Schema original) {
        super(adapter, original);
        if (original != null) {
            _props = original.getProps();
        } else {
            _props= new HashMap<>(1);
        }
    }

    @Override
    protected void setPropsInternal(Schema result) {
        if (_props != null && !_props.isEmpty()) {
            for (Map.Entry<String, String> entry : _props.entrySet()) {
                result.addProp(entry.getKey(), entry.getValue());
            }
        }
        if (result.getType() == Schema.Type.ENUM) {
            if (_defaultSymbol != null) {
                //avro 1.6 doesnt support defaults, but we can at least keep it around as a prop
                result.addProp("default", _defaultSymbol);
            }
        }
    }
}
