/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

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
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.LongNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.avro.Schema.Type.UNION;


public class Avro14AvscWriter extends AvscWriter<Jackson1JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    private static final Logger LOGGER = LoggerFactory.getLogger(Avro14AvscWriter.class);

    private static final Field SCHEMA_PROPS_FIELD;
    private static final Field FIELD_PROPS_FIELD;
    private static final Field FIELD_ALIASES_FIELD;

    static {
        try {
            SCHEMA_PROPS_FIELD = Schema.class.getDeclaredField("props");
            SCHEMA_PROPS_FIELD.setAccessible(true);
            FIELD_PROPS_FIELD = Schema.Field.class.getDeclaredField("props");
            FIELD_PROPS_FIELD.setAccessible(true);
            FIELD_ALIASES_FIELD = Schema.Field.class.getDeclaredField("aliases");
            FIELD_ALIASES_FIELD.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalStateException("unable to access props field(s) or aliases field", e);
        }
    }

    public Avro14AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
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
        Map<String, String> props = getProps(schema);
        return props != null && !props.isEmpty();
    }

    @Override
    protected void writeProps(Schema schema, Jackson1JsonGeneratorWrapper gen) throws IOException {
        Map<String, String> props = getProps(schema);
        //write all props except "default" for enums
        if (schema.getType() == Schema.Type.ENUM) {
            writeProps(props, gen, s -> !"default".equals(s));
        } else {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writeProps(Schema.Field field, Jackson1JsonGeneratorWrapper gen) throws IOException {
        Map<String, String> props = getProps(field);
        writeProps(props, gen);
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
        //avro 1.4 does not have an explicit default() API for enums, but they show up in props
        String defaultStr = enumSchema.getProp("default");
        if (defaultStr != null) {
            gen.writeStringField("default", defaultStr);
        }
    }

    @Override
    protected Set<String> getAliases(Schema.Field field) {
        try {
            //noinspection unchecked
            return (Set<String>) FIELD_ALIASES_FIELD.get(field);
        } catch (Exception e) {
            throw new IllegalStateException("unable to access aliases on field " + field, e);
        }
    }

    private Map<String, String> getProps(Schema schema) {
        try {
            //noinspection unchecked
            return (Map<String, String>) SCHEMA_PROPS_FIELD.get(schema);
        } catch (Exception e) {
            throw new IllegalStateException("unable to access props on schema " + schema, e);
        }
    }

    private Map<String, String> getProps(Schema.Field field) {
        try {
            //noinspection unchecked
            return (Map<String, String>) FIELD_PROPS_FIELD.get(field);
        } catch (Exception e) {
            throw new IllegalStateException("unable to access props on field " + field, e);
        }
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

    /**
     *  Enforces uniform numeric default values across Avro versions
     */
    private JsonNode enforceUniformNumericDefaultValues(Schema.Field field) {
        JsonNode defaultValue = field.defaultValue();
        double numericValue = defaultValue.getNumberValue().doubleValue();
        Schema schema = field.schema();
        // a default value for a union, must match the first element of the union
        Schema.Type defaultType = schema.getType() == UNION ? schema.getTypes().get(0).getType() : schema.getType();

        switch (defaultType) {
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
