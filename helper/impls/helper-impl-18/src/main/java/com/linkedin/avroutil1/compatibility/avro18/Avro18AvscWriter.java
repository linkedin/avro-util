/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import com.linkedin.avroutil1.compatibility.AvscWriter;
import com.linkedin.avroutil1.compatibility.Jackson1JsonGeneratorWrapper;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;

public class Avro18AvscWriter extends AvscWriter<Jackson1JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    public Avro18AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
        super(pretty, preAvro702, addAliasesForAvro702);
    }

    @Override
    protected Jackson1JsonGeneratorWrapper createJsonGenerator(StringWriter writer) throws IOException {
        JsonGenerator gen = FACTORY.createJsonGenerator(writer);
        if (pretty) {
            gen.useDefaultPrettyPrinter();
        }
        return new Jackson1JsonGeneratorWrapper(gen);
    }

    @Override
    protected boolean hasProps(Schema schema) {
        Map<String, JsonNode> props = schema.getJsonProps();
        return props != null && !props.isEmpty();
    }

    @Override
    protected void writeProps(Schema schema, Jackson1JsonGeneratorWrapper gen) throws IOException {
        Map<String, JsonNode> props = schema.getJsonProps();
        if (props != null && !props.isEmpty()) {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writeProps(Schema.Field field, Jackson1JsonGeneratorWrapper gen) throws IOException {
        Map<String, JsonNode> props = field.getJsonProps();
        if (props != null && !props.isEmpty()) {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writeDefaultValue(Schema.Field field, Jackson1JsonGeneratorWrapper gen) throws IOException {
        JsonNode defaultValue = field.defaultValue();
        if (defaultValue != null) {
            gen.writeFieldName("default");
            gen.getDelegate().writeTree(defaultValue);
        }
    }

    @Override
    protected Set<String> getAliases(Schema.Field field) {
        return field.aliases();
    }

    private void writeProps(Map<String, JsonNode> props, Jackson1JsonGeneratorWrapper gen) throws IOException {
        JsonGenerator delegate = gen.getDelegate();
        for (Map.Entry<String, JsonNode> entry : props.entrySet()) {
            delegate.writeObjectField(entry.getKey(), entry.getValue());
        }
    }
}
