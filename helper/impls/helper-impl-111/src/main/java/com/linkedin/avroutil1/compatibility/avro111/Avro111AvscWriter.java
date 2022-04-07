/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.linkedin.avroutil1.compatibility.AvscWriter;
import com.linkedin.avroutil1.compatibility.Jackson2JsonGeneratorWrapper;
import org.apache.avro.Schema;
import org.apache.avro.util.internal.Accessor;
import org.apache.avro.util.internal.JacksonUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.avro.Schema.Type.*;


public class Avro111AvscWriter extends AvscWriter<Jackson2JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    private static final Logger LOGGER = LoggerFactory.getLogger(Avro111AvscWriter.class);

    public Avro111AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
        super(pretty, preAvro702, addAliasesForAvro702);
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
            JsonNode defaultValue = Accessor.defaultValue(field);
            if (defaultValue.isNumber()) {
                defaultValue = enforceUniformNumericDefaultValues(field);
            }
            gen.writeFieldName("default");
            gen.getDelegate().writeTree(defaultValue);
        }
    }

    @Override
    protected void writeEnumDefault(Schema enumSchema, Jackson2JsonGeneratorWrapper gen) throws IOException {
        String defaultStr = enumSchema.getEnumDefault();
        if (defaultStr != null) {
            gen.writeStringField("default", defaultStr);
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

    /**
     *  Enforces uniform numeric default values across Avro versions
     */
    private JsonNode enforceUniformNumericDefaultValues(Schema.Field field) {
        JsonNode genericDefaultValue = Accessor.defaultValue(field);
        double numericDefaultValue = genericDefaultValue.doubleValue();
        Schema schema = field.schema();
        // a default value for a union, must match the first element of the union
        Schema.Type defaultType = schema.getType() == UNION ? schema.getTypes().get(0).getType() : schema.getType();

        switch (defaultType) {
            case INT:
                if (numericDefaultValue % 1 != 0) {
                    LOGGER.warn(String.format("Invalid default value: %s for \"int\" field: %s", genericDefaultValue, field.name()));
                    return genericDefaultValue;
                }
                return new IntNode(genericDefaultValue.intValue());
            case LONG:
                if (numericDefaultValue % 1 != 0) {
                    LOGGER.warn(String.format("Invalid default value: %s for \"long\" field: %s", genericDefaultValue, field.name()));
                    return genericDefaultValue;
                }
                return new LongNode(genericDefaultValue.longValue());
            case DOUBLE:
                return new DoubleNode(genericDefaultValue.doubleValue());
            case FLOAT:
                return new FloatNode(genericDefaultValue.floatValue());
            default:
                return genericDefaultValue;
        }
    }
}
