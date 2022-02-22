/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.AbstractSchemaBuilder;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonNode;

import java.util.Map;
import org.codehaus.jackson.node.TextNode;


public class SchemaBuilder17 extends AbstractSchemaBuilder {

    private Map<String, JsonNode> _props;

    public SchemaBuilder17(AvroAdapter adapter, Schema original) {
        super(adapter, original);
        if (original != null) {
            _props = Avro17Utils.getProps(original);
        } else {
            _props= new HashMap<>(1);
        }
    }

    @Override
    protected void setPropsInternal(Schema result) {
        if (_props != null && !_props.isEmpty()) {
            Avro17Utils.setProps(result, _props);
        }
        if (result.getType() == Schema.Type.ENUM) {
            if (_defaultSymbol != null) {
                //avro 1.7 doesnt support defaults, but we can at least keep it around as a prop
                Map<String, JsonNode> defaultProp = new HashMap<>(1);
                defaultProp.put("default", new TextNode(_defaultSymbol));
                Avro17Utils.setProps(result, defaultProp);
            }
        }
    }
}
