/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import com.linkedin.avroutil1.compatibility.AbstractSchemaBuilder;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import java.util.HashMap;
import org.apache.avro.Schema;

import java.lang.reflect.Field;
import java.util.Map;

public class SchemaBuilder15 extends AbstractSchemaBuilder {
    private final static Field SCHEMA_PROPS_FIELD;

    static {
        try {
            Class<Schema> schemaClass = Schema.class;
            SCHEMA_PROPS_FIELD = schemaClass.getDeclaredField("props");
            SCHEMA_PROPS_FIELD.setAccessible(true);
        } catch (Throwable issue) {
            throw new IllegalStateException("unable to find/access Schema.props", issue);
        }
    }

    private Map<String, String> _props;

    public SchemaBuilder15(AvroAdapter adapter, Schema original) {
        super(adapter, original);
        if (original != null) {
            _props = getProps(original);
        } else {
            _props= new HashMap<>(1);
        }
    }

    @Override
    protected void setPropsInternal(Schema result) {
        if (_props != null && !_props.isEmpty()) {
            getProps(result).putAll(_props);
        }
        if (result.getType() == Schema.Type.ENUM) {
            if (_defaultSymbol != null) {
                //avro 1.5 doesnt support defaults, but we can at least keep it around as a prop
                getProps(result).put("default", _defaultSymbol);
            }
        }
    }

    private Map<String,String> getProps(Schema schema) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, String> props = (Map<String, String>) SCHEMA_PROPS_FIELD.get(schema);
            return props;
        } catch (Exception e) {
            throw new IllegalStateException("unable to access props on Schema " + schema.getFullName(), e);
        }
    }
}
