/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvscWriter;
import com.linkedin.avroutil1.compatibility.Jackson2JsonGeneratorWrapper;
import org.apache.avro.Schema;
import org.apache.avro.util.internal.JacksonUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;

public class Avro19AvscWriter extends AvscWriter<Jackson2JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    public Avro19AvscWriter(boolean pretty, boolean preAvro702) {
        super(pretty, preAvro702);
    }

    @Override
    protected Jackson2JsonGeneratorWrapper createJsonGenerator(StringWriter writer) throws IOException {
        JsonGenerator gen = FACTORY.createGenerator(writer);
        if (pretty) {
            gen.useDefaultPrettyPrinter();
        }
        return new Jackson2JsonGeneratorWrapper(gen);
    }

    @Override
    protected boolean hasProps(Schema schema) {
        return schema.hasProps();
    }

    @Override
    protected void writeProps(Schema schema, Jackson2JsonGeneratorWrapper gen) throws IOException {
        Map<String, Object> props = schema.getObjectProps();
        if (props != null && !props.isEmpty()) {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writeProps(Schema.Field field, Jackson2JsonGeneratorWrapper gen) throws IOException {
        Map<String, Object> props = field.getObjectProps();
        if (props != null && !props.isEmpty()) {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writeDefaultValue(Schema.Field field, Jackson2JsonGeneratorWrapper gen) throws IOException {
        if (field.hasDefaultValue()) {
            gen.writeFieldName("default");
            Object o = field.defaultVal();
            gen.getDelegate().writeTree(JacksonUtils.toJsonNode(o));
        }
    }

    @Override
    protected Set<String> getAliases(Schema.Field field) {
        return field.aliases();
    }

    private void writeProps(Map<String, Object> props, Jackson2JsonGeneratorWrapper gen) throws IOException {
        JsonGenerator delegate = gen.getDelegate();
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            Object o = entry.getValue();
            delegate.writeObjectField(entry.getKey(), JacksonUtils.toJsonNode(o));
        }
    }
}
