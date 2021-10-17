/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.JsonPropertiesContainer;
import com.linkedin.avroutil1.parser.jsonpext.JsonValueExt;
import com.linkedin.avroutil1.util.Util;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class JsonPropertiesContainerImpl implements JsonPropertiesContainer {
    private final static JsonWriterFactory JSON_FACTORY;

    static {
        //this is here so we know the exact writer configs we get
        Map<String, Object> config = new HashMap<>();
        JSON_FACTORY = Json.createWriterFactory(config);
    }

    private final LinkedHashMap<String, JsonValueExt> props;

    JsonPropertiesContainerImpl(LinkedHashMap<String, JsonValueExt> props) {
        this.props = props;
    }

    @Override
    public Set<String> propertyNames() {
        return props.keySet();
    }

    @Override
    public Object getPropertyAsObject(String key) {
        JsonValueExt value = props.get(key);
        if (value == null) {
            return null;
        }
        return Util.convertJsonValue(value);
    }

    @Override
    public String getPropertyAsJsonLiteral(String key) {
        JsonValueExt value = props.get(key);
        if (value == null) {
            return null;
        }
        StringWriter sw = new StringWriter();
        try (JsonWriter writer = JSON_FACTORY.createWriter(sw)) {
            writer.write(value);
        }
        return sw.toString();
    }

    @Override
    public String toString() {
        if (props.isEmpty()) {
            return "{}";
        }
        JsonObjectBuilder ob = Json.createObjectBuilder();
        props.forEach(ob::add);
        JsonObject object = ob.build();
        StringWriter sw = new StringWriter();
        try (JsonWriter writer = JSON_FACTORY.createWriter(sw)) {
            writer.write(object);
        }
        return sw.toString();
    }
}
