/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.compatibility.AvscWriter;
import com.linkedin.avroutil1.compatibility.Jackson1JsonGeneratorWrapper;
import java.util.function.Predicate;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;

public class Avro16AvscWriter extends AvscWriter<Jackson1JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    public Avro16AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
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
        Map<String, String> props = schema.getProps();
        return props != null && !props.isEmpty();
    }

    @Override
    protected void writeProps(Schema schema, Jackson1JsonGeneratorWrapper gen) throws IOException {
        Map<String, String> props = schema.getProps();
        //write all props except "default" for enums
        if (schema.getType() == Schema.Type.ENUM) {
            writeProps(props, gen, s -> !"default".equals(s));
        } else {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writeProps(Schema.Field field, Jackson1JsonGeneratorWrapper gen) throws IOException {
        Map<String, String> props = field.props();
        writeProps(props, gen);
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
    protected void writeEnumDefault(Schema enumSchema, Jackson1JsonGeneratorWrapper gen) throws IOException {
        //avro 1.6 does not have an explicit default() API for enums, but they show up in props
        String defaultStr = enumSchema.getProp("default");
        if (defaultStr != null) {
            gen.writeStringField("default", defaultStr);
        }
    }

    @Override
    protected Set<String> getAliases(Schema.Field field) {
        return field.aliases();
    }

    private void writeProps(Map<String, String> props, Jackson1JsonGeneratorWrapper gen) throws IOException {
        writeProps(props, gen, null);
    }

    private void writeProps(
        Map<String, String> props,
        Jackson1JsonGeneratorWrapper gen,
        Predicate<String> propNameFilter
    ) throws IOException {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String propName = entry.getKey();
            if (propNameFilter == null || propNameFilter.test(propName)) {
                gen.writeStringField(propName, entry.getValue());
            }
        }
    }
}
