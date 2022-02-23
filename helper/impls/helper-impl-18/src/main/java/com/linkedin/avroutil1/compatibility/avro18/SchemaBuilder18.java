/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import com.linkedin.avroutil1.compatibility.AbstractSchemaBuilder;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.Map;
import org.codehaus.jackson.node.TextNode;


public class SchemaBuilder18 extends AbstractSchemaBuilder {

    private Map<String, JsonNode> _props;

    public SchemaBuilder18(AvroAdapter adapter, Schema original) {
        super(adapter, original);
        if (original != null) {
            //noinspection deprecation
            _props = original.getJsonProps(); //actually faster
        } else {
            _props= new HashMap<>(1);
        }
    }

    @Override
    protected void setPropsInternal(Schema result) {
        if (_props != null && !_props.isEmpty()) {
            for (Map.Entry<String, JsonNode> entry : _props.entrySet()) {
                result.addProp(entry.getKey(), entry.getValue()); //deprecated but faster
            }
        }
        if (result.getType() == Schema.Type.ENUM) {
            if (_defaultSymbol != null) {
                //avro 1.8 doesnt support defaults, but we can at least keep it around as a prop
                result.addProp("default", new TextNode(_defaultSymbol));
            }
        }
    }
}
