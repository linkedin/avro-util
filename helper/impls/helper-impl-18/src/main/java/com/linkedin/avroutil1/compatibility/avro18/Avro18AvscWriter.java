/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import com.linkedin.avroutil1.compatibility.AvscWriter;
import com.linkedin.avroutil1.compatibility.Jackson1JsonGeneratorWrapper;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


public class Avro18AvscWriter extends AvscWriter<Jackson1JsonGeneratorWrapper> {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());

    public Avro18AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
        super(pretty, preAvro702, addAliasesForAvro702);
    }

    public Avro18AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702, boolean retainDefaults,
        boolean retainDocs, boolean retainFieldAliases, boolean retainNonClaimedProps, boolean retainSchemaAliases,
        boolean writeNamespaceExplicitly, boolean writeRelativeNamespace, List<AvscWriterPlugin> schemaPlugins) {
        super(pretty, preAvro702, addAliasesForAvro702, retainDefaults, retainDocs, retainFieldAliases,
            retainNonClaimedProps, retainSchemaAliases, writeNamespaceExplicitly, writeRelativeNamespace,
            schemaPlugins);
    }

    public Avro18AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702, boolean retainDefaults,
        boolean retainDocs, boolean retainFieldAliases, boolean retainNonClaimedProps, boolean retainSchemaAliases,
        boolean writeNamespaceExplicitly, boolean writeRelativeNamespace, boolean isLegacy,
        List<AvscWriterPlugin> schemaPlugins) {
        super(pretty, preAvro702, addAliasesForAvro702, retainDefaults, retainDocs, retainFieldAliases,
            retainNonClaimedProps, retainSchemaAliases, writeNamespaceExplicitly, writeRelativeNamespace, isLegacy,
            schemaPlugins);
    }

    @Override
    protected Jackson1JsonGeneratorWrapper createJsonGenerator(Writer writer) throws IOException {
        JsonGenerator gen = FACTORY.createJsonGenerator(writer);
        return createWrapper(gen);
    }

    @Override
    protected Jackson1JsonGeneratorWrapper createJsonGenerator(OutputStream stream) throws IOException {
        JsonGenerator gen = FACTORY.createJsonGenerator(stream);
        return createWrapper(gen);
    }

    private Jackson1JsonGeneratorWrapper createWrapper(JsonGenerator generator) {
        if (pretty) {
            generator.useDefaultPrettyPrinter();
        }
        return new Jackson1JsonGeneratorWrapper(generator);
    }

    @Override
    protected boolean hasProps(Schema schema) {
        Map<String, JsonNode> props = schema.getJsonProps();
        return props != null && !props.isEmpty();
    }

    @Override
    protected void writeProps(Schema schema, Jackson1JsonGeneratorWrapper gen, Set<String> propNames) throws IOException {
        Map<String, JsonNode> props = schema.getJsonProps();
        if (props == null || props.isEmpty()) {
            return;
        }
        Map<String, JsonNode> sortedProps = new TreeMap<>();
        for(String propName : propNames) {
            sortedProps.put(propName, props.get(propName));
        }
        //write all props except "default" for enums
        if (schema.getType() == Schema.Type.ENUM) {
            writeProps(sortedProps, gen, s -> !"default".equals(s));
        } else {
            writeProps(sortedProps, gen);
        }
    }

    @Override
    protected void writeProps(Schema.Field field, Jackson1JsonGeneratorWrapper gen, Set<String> propNames) throws IOException {
        Map<String, JsonNode> props = field.getJsonProps();
        if (props != null && !props.isEmpty()) {
            Map<String, JsonNode> sortedProps = new TreeMap<>();
            for(String propName : propNames) {
                sortedProps.put(propName, props.get(propName));
            }
            writeProps(sortedProps, gen);
        }
    }

    @Override
    protected void writeDefaultValue(Schema.Field field, Jackson1JsonGeneratorWrapper gen) throws IOException {
        JsonNode defaultValue = field.defaultValue();
        if (defaultValue != null) {
            if (defaultValue.isNumber()) {
                defaultValue = Jackson1Utils.enforceUniformNumericDefaultValues(field);
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

    @Override
    protected List<String> getAllPropNames(Schema schema) {
        return new Avro18Adapter().getAllPropNames(schema);
    }

    @Override
    protected List<String> getAllPropNames(Schema.Field field) {
        return new Avro18Adapter().getAllPropNames(field);
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
                delegate.writeObjectField(entry.getKey(), Jackson1Utils.sortJsonNode(entry.getValue()));
            }
        }
    }

    @Override
    protected void writePropsLegacy(Schema schema, Jackson1JsonGeneratorWrapper gen, Set<String> propNames) throws IOException {
        Map<String, JsonNode> props = schema.getJsonProps();
        if (props == null || props.isEmpty()) {
            return;
        }
        props = new LinkedHashMap<>(props);
        props.entrySet().removeIf(e -> !propNames.contains(e.getKey()));
        //write all props except "default" for enums
        if (schema.getType() == Schema.Type.ENUM) {
            writeProps(props, gen, s -> !"default".equals(s));
        } else {
            writeProps(props, gen);
        }
    }

    @Override
    protected void writePropsLegacy(Schema.Field field, Jackson1JsonGeneratorWrapper gen, Set<String> propNames) throws IOException {
        Map<String, JsonNode> props = field.getJsonProps();
        if (props == null) {
            return;
        }
        props = new LinkedHashMap<>(props);
        props.entrySet().removeIf(e -> !propNames.contains(e.getKey()));
        if (!props.isEmpty()) {
            writeProps(props, gen);
        }
    }
}
