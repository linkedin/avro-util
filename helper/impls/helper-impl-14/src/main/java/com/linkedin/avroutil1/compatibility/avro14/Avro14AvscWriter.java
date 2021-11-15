/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.avro14.backports.Avro111Name;
import com.linkedin.avroutil1.compatibility.avro14.backports.Avro111Names;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Avro14AvscWriter {
    private static final JsonFactory FACTORY = new JsonFactory().setCodec(new ObjectMapper());
    private static final Field SCHEMA_PROPS_FIELD;
    private static final Field FIELD_PROPS_FIELD;
    private static final Field FIELD_ALIASES_FIELD;
    private static final Set<Schema.Type> NAMED_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            Schema.Type.ENUM,
            Schema.Type.FIXED,
            Schema.Type.RECORD
    )));

    private final boolean pretty;

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

    public Avro14AvscWriter(boolean pretty) {
        this.pretty = pretty;
    }

    public String toAvsc(Schema schema) {
        try {
            Avro111Names names = new Avro111Names();
            StringWriter writer = new StringWriter();
            JsonGenerator gen = FACTORY.createJsonGenerator(writer);
            if (pretty) {
                gen.useDefaultPrettyPrinter();
            }
            toJson(schema, names, gen);
            gen.flush();
            return writer.toString();
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }

    private void toJson(Schema schema, Avro111Names names, JsonGenerator gen) throws IOException {
        switch (schema.getType()) {
            case ENUM:
                //taken from EnumSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                gen.writeStartObject();
                gen.writeStringField("type", "enum");
                writeName(schema, names, gen);
                if (schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                gen.writeArrayFieldStart("symbols");
                for (String symbol : schema.getEnumSymbols()) {
                    gen.writeString(symbol);
                }
                gen.writeEndArray();
                //avro 1.4 doesnt natively support enum defaults, but they should show up as props
                //if (schema.getEnumDefault() != null) {
                //    gen.writeStringField("default", getEnumDefault());
                //}
                writeProps(schema, gen);
                aliasesToJson(schema, gen);
                gen.writeEndObject();
                break;
            case FIXED:
                //taken from FixedSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                gen.writeStartObject();
                gen.writeStringField("type", "fixed");
                writeName(schema, names, gen);
                if (schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                gen.writeNumberField("size", schema.getFixedSize());
                writeProps(schema, gen);
                aliasesToJson(schema, gen);
                gen.writeEndObject();
                break;
            case RECORD:
                //taken from RecordSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                String savedSpace = names.space(); // save namespace
                gen.writeStartObject();
                gen.writeStringField("type", schema.isError() ? "error" : "record");
                writeName(schema, names, gen);
                Avro111Name name = nameOf(schema);
                names.space(name.getSpace()); // set default namespace
                if (schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                if (schema.getFields() != null) {
                    gen.writeFieldName("fields");
                    fieldsToJson(schema, names, gen);
                }
                writeProps(schema, gen);
                aliasesToJson(schema, gen);
                gen.writeEndObject();
                names.space(savedSpace); // restore namespace
                break;
            case ARRAY:
                //taken from ArraySchema.toJson() in avro 1.11
                gen.writeStartObject();
                gen.writeStringField("type", "array");
                gen.writeFieldName("items");
                toJson(schema.getElementType(), names, gen);
                writeProps(schema, gen);
                gen.writeEndObject();
                break;
            case MAP:
                //taken from MapSchema.toJson() in avro 1.11
                gen.writeStartObject();
                gen.writeStringField("type", "map");
                gen.writeFieldName("values");
                toJson(schema.getValueType(), names, gen);
                writeProps(schema, gen);
                gen.writeEndObject();
                break;
            case UNION:
                //taken from UnionSchema.toJson() in avro 1.11
                gen.writeStartArray();
                for (Schema type : schema.getTypes()) {
                    toJson(type, names, gen);
                }
                gen.writeEndArray();
                break;
            default:
                //all other schema types (taken from Schema.toJson() in avro 1.11)
                if (!hasProps(schema)) { // no props defined
                    gen.writeString(schema.getName()); // just write name
                } else {
                    gen.writeStartObject();
                    gen.writeStringField("type", schema.getName());
                    writeProps(schema, gen);
                    gen.writeEndObject();
                }
        }
    }

    private static void writeProps(Schema schema, JsonGenerator gen) throws IOException {
        Map<String, String> props = getProps(schema);
        writeProps(props, gen);
    }

    private static void writeProps(Schema.Field field, JsonGenerator gen) throws IOException {
        Map<String, String> props = getProps(field);
        writeProps(props, gen);
    }

    private static void writeProps(Map<String, String> props, JsonGenerator gen) throws IOException {
        for (Map.Entry<String, String> entry : props.entrySet()) {
            gen.writeStringField(entry.getKey(), entry.getValue());
        }
    }

    private static void aliasesToJson(Schema schema, JsonGenerator gen) throws IOException {
        Set<String> aliases = schema.getAliases();
        if (aliases == null || aliases.size() == 0) {
            return;
        }
        Avro111Name name = nameOf(schema);
        gen.writeFieldName("aliases");
        gen.writeStartArray();
        for (String s : aliases) {
            Avro111Name alias = new Avro111Name(s, null);
            gen.writeString(alias.getQualified(name.getSpace()));
        }
        gen.writeEndArray();
    }

    void fieldsToJson(Schema schema, Avro111Names names, JsonGenerator gen) throws IOException {
        gen.writeStartArray();
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field f : fields) {
            gen.writeStartObject();
            gen.writeStringField("name", f.name());
            gen.writeFieldName("type");
            toJson(f.schema(), names, gen);
            if (f.doc() != null) {
                gen.writeStringField("doc", f.doc());
            }
            JsonNode defaultValue = f.defaultValue();
            if (defaultValue != null) {
                gen.writeFieldName("default");
                gen.writeTree(defaultValue);
            }
            if (f.order() != Schema.Field.Order.ASCENDING) {
                gen.writeStringField("order", f.order().name());
            }
            Set<String> aliases = getAliases(f);
            if (aliases != null && aliases.size() != 0) {
                gen.writeFieldName("aliases");
                gen.writeStartArray();
                for (String alias : aliases) {
                    gen.writeString(alias);
                }
                gen.writeEndArray();
            }
            writeProps(f, gen);
            gen.writeEndObject();
        }
        gen.writeEndArray();
    }

    //returns true if this schema (by fqcn) is in names, hence has been written before, and so now
    //just a "ref" (fqcn string) will do
    public static boolean writeNameRef(Schema schema, Avro111Names names, JsonGenerator gen) throws IOException {
        Avro111Name name = nameOf(schema);
        if (schema.equals(names.get(name))) {
            gen.writeString(name.getQualified(names.space()));
            return true;
        } else if (!name.isAnonymous()) {
            names.put(name, schema);
        }
        return false;
    }

    public static void writeName(Schema schema, Avro111Names names, JsonGenerator gen) throws IOException {
        Avro111Name name = nameOf(schema);
        name.writeName(names, gen);
    }

    private static Avro111Name nameOf(Schema schema) {
        Schema.Type type = schema.getType();
        if (!NAMED_TYPES.contains(type)) {
            throw new IllegalArgumentException("dont know how to build a Name out of " + type + " " + schema);
        }
        return new Avro111Name(schema.getName(), schema.getNamespace());
    }

    private static boolean hasProps(Schema schema) {
        Map<String, String> props = getProps(schema);
        return props != null && !props.isEmpty();
    }

    private static Map<String, String> getProps(Schema schema) {
        try {
            //noinspection unchecked
            return (Map<String, String>) SCHEMA_PROPS_FIELD.get(schema);
        } catch (Exception e) {
            throw new IllegalStateException("unable to access props on schema " + schema, e);
        }
    }

    private static Map<String, String> getProps(Schema.Field field) {
        try {
            //noinspection unchecked
            return (Map<String, String>) FIELD_PROPS_FIELD.get(field);
        } catch (Exception e) {
            throw new IllegalStateException("unable to access props on field " + field, e);
        }
    }

    private static Set<String> getAliases(Schema.Field field) {
        try {
            //noinspection unchecked
            return (Set<String>) FIELD_ALIASES_FIELD.get(field);
        } catch (Exception e) {
            throw new IllegalStateException("unable to access aliases on field " + field, e);
        }
    }
}
