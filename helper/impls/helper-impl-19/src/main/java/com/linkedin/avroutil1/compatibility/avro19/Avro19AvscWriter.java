/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvscWriter;
import com.linkedin.avroutil1.compatibility.Jackson2JsonGeneratorWrapper;
import com.linkedin.avroutil1.compatibility.Jackson2Utils;
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.util.internal.Accessor;
import org.apache.avro.util.internal.JacksonUtils;


public class Avro19AvscWriter extends AvscWriter<Jackson2JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    public Avro19AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
        super(pretty, preAvro702, addAliasesForAvro702);
    }

    public Avro19AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702, boolean retainDefaults,
        boolean retainDocs, boolean retainFieldAliases, boolean retainNonClaimedProps, boolean retainSchemaAliases,
        boolean writeNamespaceExplicitly, boolean writeRelativeNamespace, List<AvscWriterPlugin> schemaPlugins) {
        super(pretty, preAvro702, addAliasesForAvro702, retainDefaults, retainDocs, retainFieldAliases,
            retainNonClaimedProps, retainSchemaAliases, writeNamespaceExplicitly, writeRelativeNamespace,
            schemaPlugins);
    }

    public Avro19AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702, boolean retainDefaults,
        boolean retainDocs, boolean retainFieldAliases, boolean retainNonClaimedProps, boolean retainSchemaAliases,
        boolean writeNamespaceExplicitly, boolean writeRelativeNamespace, boolean isLegacy,
        List<AvscWriterPlugin> schemaPlugins) {
        super(pretty, preAvro702, addAliasesForAvro702, retainDefaults, retainDocs, retainFieldAliases,
            retainNonClaimedProps, retainSchemaAliases, writeNamespaceExplicitly, writeRelativeNamespace, isLegacy,
            schemaPlugins);
    }

    @Override
    protected Jackson2JsonGeneratorWrapper createJsonGenerator(Writer writer) throws IOException {
        JsonGenerator gen = FACTORY.createGenerator(writer);
        return createWrapper(gen);
    }

    @Override
    protected Jackson2JsonGeneratorWrapper createJsonGenerator(OutputStream stream) throws IOException {
        JsonGenerator gen = FACTORY.createGenerator(stream);
        return createWrapper(gen);
    }

    private Jackson2JsonGeneratorWrapper createWrapper(JsonGenerator generator) {
        if (pretty) {
            generator.useDefaultPrettyPrinter();
        }
        return new Jackson2JsonGeneratorWrapper(generator);
    }

    @Override
    protected boolean hasProps(Schema schema) {
        return schema.hasProps();
    }

    @Override
    protected void writeProps(Schema schema, Jackson2JsonGeneratorWrapper gen, Set<String> propNames) throws IOException {
        writeAllowedProps(gen, propNames, schema.getObjectProps());
    }

    @Override
    protected void writeProps(Schema.Field field, Jackson2JsonGeneratorWrapper gen, Set<String> propNames) throws IOException {
        writeAllowedProps(gen, propNames, field.getObjectProps());
    }

    private void writeAllowedProps(Jackson2JsonGeneratorWrapper gen, Set<String> allowedProps,
        Map<String, Object> objectProps) throws IOException {
        Map<String, Object> props = objectProps;
        if (props != null && !props.isEmpty()) {
            Map<String, Object> sortedProps = new TreeMap<>();
            for(String propName : allowedProps) {
                sortedProps.put(propName, props.get(propName));
            }
            writeProps(sortedProps, gen);
        }
    }

    @Override
    protected void writeDefaultValue(Schema.Field field, Jackson2JsonGeneratorWrapper gen) throws IOException {
        if (field.hasDefaultValue()) {
            JsonNode defaultValue = Accessor.defaultValue(field);
            if (defaultValue.isNumber()) {
                defaultValue = Jackson2Utils.enforceUniformNumericDefaultValues(field, defaultValue);
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

    @Override
    protected List<String> getAllPropNames(Schema schema) {
        return new Avro19Adapter().getAllPropNames(schema);
    }

    @Override
    protected List<String> getAllPropNames(Schema.Field field) {
        return new Avro19Adapter().getAllPropNames(field);
    }

    private void writeProps(Map<String, Object> props, Jackson2JsonGeneratorWrapper gen) throws IOException {
        JsonGenerator delegate = gen.getDelegate();
        for (Map.Entry<String, Object> entry : props.entrySet()) {
            Object o = entry.getValue();
            delegate.writeObjectField(entry.getKey(), Jackson2Utils.sortJsonNode(JacksonUtils.toJsonNode(o)));
        }
    }
    @Override
    protected void writePropsLegacy(Schema schema, Jackson2JsonGeneratorWrapper gen, Set<String> propNames)
        throws IOException {
        Map<String, Object> props = new LinkedHashMap<>(schema.getObjectProps());
        props.entrySet().removeIf(e -> !propNames.contains(e.getKey()));
        if (props != null && !props.isEmpty()) {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writePropsLegacy(Schema.Field field, Jackson2JsonGeneratorWrapper gen, Set<String> propNames)
        throws IOException {
        Map<String, Object> props = new LinkedHashMap<>(field.getObjectProps());
        props.entrySet().removeIf(e -> !propNames.contains(e.getKey()));
        if (props != null && !props.isEmpty()) {
            writeProps(props, gen);
        }
    }

}
