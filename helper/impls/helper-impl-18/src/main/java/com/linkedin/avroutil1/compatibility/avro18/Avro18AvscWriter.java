/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

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
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Avro18AvscWriter extends AvscWriter<Jackson1JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    private static final Logger LOGGER = LoggerFactory.getLogger(Avro18AvscWriter.class);

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
        if (props == null || props.isEmpty()) {
            return;
        }
        //write all props except "default" for enums
        if (schema.getType() == Schema.Type.ENUM) {
            writeProps(props, gen, s -> !"default".equals(s));
        } else {
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
            if (defaultValue.isNumber()) {
                defaultValue = enforceUniformNumericDefaultValues(field);
            }
            gen.writeFieldName("default");
            gen.getDelegate().writeTree(defaultValue);
        }
    }

    @Override
    protected void writeEnumDefault(Schema enumSchema, Jackson1JsonGeneratorWrapper gen) throws IOException {
        //avro 1.8 does not have an explicit default() API for enums, but they show up in props
        String defaultStr = enumSchema.getProp("default");
        if (defaultStr != null) {
            gen.writeStringField("default", defaultStr);
        }
    }

    @Override
    protected Set<String> getAliases(Schema.Field field) {
        return field.aliases();
    }

    private void writeProps(Map<String, JsonNode> props, Jackson1JsonGeneratorWrapper gen) throws IOException {
        writeProps(props, gen, null);
    }

    private void writeProps(
        Map<String, JsonNode> props,
        Jackson1JsonGeneratorWrapper gen,
        Predicate<String> propNameFilter
    ) throws IOException {
        JsonGenerator delegate = gen.getDelegate();
        for (Map.Entry<String, JsonNode> entry : props.entrySet()) {
            String propName = entry.getKey();
            if (propNameFilter == null || propNameFilter.test(propName)) {
                delegate.writeObjectField(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     *  Enforces uniform numeric default values across Avro versions
     */
    private JsonNode enforceUniformNumericDefaultValues(Schema.Field field) {
        JsonNode defaultValue = field.defaultValue();
        double numericValue = defaultValue.getNumberValue().doubleValue();
        switch (field.schema().getType()) {
            case INT:
                if (numericValue % 1 != 0) {
                    LOGGER.warn(String.format("Invalid default value: %s for \"int\" field: %s", field.defaultValue(), field.name()));
                    return defaultValue;
                }
                return new IntNode(defaultValue.getNumberValue().intValue());
            case LONG:
                if (numericValue % 1 != 0) {
                    LOGGER.warn(String.format("Invalid default value: %s for \"long\" field: %s", field.defaultValue(), field.name()));
                    return defaultValue;
                }
                return new LongNode(defaultValue.getNumberValue().longValue());
            case DOUBLE:
                return new DoubleNode(defaultValue.getNumberValue().doubleValue());
            case FLOAT:
                return new DoubleNode(defaultValue.getNumberValue().floatValue());
            default:
                return defaultValue;
        }
    }
}
