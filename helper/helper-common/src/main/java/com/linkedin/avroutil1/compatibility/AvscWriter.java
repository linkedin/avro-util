/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.compatibility.backports.Avro702Data;
import com.linkedin.avroutil1.compatibility.backports.AvroName;
import com.linkedin.avroutil1.compatibility.backports.AvroNames;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AvscWriter<G extends JsonGeneratorWrapper<?>> {
    /**
     * true for pretty-printing (newlines and indentation)
     * false foa one-liner (good for SCHEMA$)
     */
    protected final boolean pretty;
    /**
     * true to produce potentially bad avsc for compatibility with avro 1.4
     * false to produce proper, correct, avsc.
     */
    protected final boolean preAvro702;
    /**
     * true adds aliases to all named types (record, enum, fixed) to their "other fullname".
     * "other fullname" is either the correct one of the avro702-impacted one, depending
     * on the value of {@link #preAvro702}
     */
    protected final boolean addAliasesForAvro702;

    protected AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
        this.pretty = pretty;
        this.preAvro702 = preAvro702;
        this.addAliasesForAvro702 = addAliasesForAvro702;
    }

    public String toAvsc(Schema schema) {
        try {
            AvroNames names = new AvroNames();
            StringWriter writer = new StringWriter();
            G gen = createJsonGenerator(writer);
            toJson(schema, names, "", "", gen);
            gen.flush();
            return writer.toString();
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }

    protected void toJson(
        Schema schema,
        AvroNames names,
        String contextNamespaceWhenParsed,
        String contextNamespaceWhenParsedUnder702,
        G gen
    ) throws IOException {
        Avro702Data avro702Data;
        switch (schema.getType()) {
            case ENUM:
                //taken from EnumSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                gen.writeStartObject();
                gen.writeStringField("type", "enum");
                avro702Data = writeName(schema, names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                if (schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                gen.writeArrayFieldStart("symbols");
                for (String symbol : schema.getEnumSymbols()) {
                    gen.writeString(symbol);
                }
                gen.writeEndArray();
                writeEnumDefault(schema, gen);
                writeProps(schema, gen);
                aliasesToJson(schema, avro702Data.getExtraAliases(), gen);
                gen.writeEndObject();
                break;
            case FIXED:
                //taken from FixedSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                gen.writeStartObject();
                gen.writeStringField("type", "fixed");
                avro702Data = writeName(schema, names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                if (schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                gen.writeNumberField("size", schema.getFixedSize());
                writeProps(schema, gen);
                aliasesToJson(schema, avro702Data.getExtraAliases(), gen);
                gen.writeEndObject();
                break;
            case RECORD:
                //taken from RecordSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                gen.writeStartObject();
                gen.writeStringField("type", schema.isError() ? "error" : "record");
                avro702Data = writeName(schema, names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                AvroName name = AvroName.of(schema);

                //save current namespaces - both 1.4 and correct one
                String savedBadSpace = names.badSpace(); //save avro-702 mode namespace
                String savedCorrectSpace = names.correctSpace(); //save correct namespace

                //avro 1.4 only ever sets namespace if the current is null
                if (savedBadSpace == null) {
                    names.badSpace(name.getSpace());
                }
                names.correctSpace(name.getSpace()); //always update correct namespace

                if (schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                if (schema.getFields() != null) {
                    gen.writeFieldName("fields");
                    fieldsToJson(
                        schema,
                        names,
                        avro702Data.getNamespaceWhenParsing(),
                        avro702Data.getNamespaceWhenParsing702(),
                        gen
                    );
                }
                writeProps(schema, gen);
                aliasesToJson(schema, avro702Data.getExtraAliases(), gen);
                gen.writeEndObject();
                //avro 1.4 never restores namespace, so we never restore space
                names.correctSpace(savedCorrectSpace); //always restore correct namespace
                break;
            case ARRAY:
                //taken from ArraySchema.toJson() in avro 1.11
                gen.writeStartObject();
                gen.writeStringField("type", "array");
                gen.writeFieldName("items");
                toJson(schema.getElementType(), names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                writeProps(schema, gen);
                gen.writeEndObject();
                break;
            case MAP:
                //taken from MapSchema.toJson() in avro 1.11
                gen.writeStartObject();
                gen.writeStringField("type", "map");
                gen.writeFieldName("values");
                toJson(schema.getValueType(), names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                writeProps(schema, gen);
                gen.writeEndObject();
                break;
            case UNION:
                //taken from UnionSchema.toJson() in avro 1.11
                gen.writeStartArray();
                for (Schema type : schema.getTypes()) {
                    toJson(type, names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
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

    protected void aliasesToJson(Schema schema, List<AvroName> extraAliases, G gen) throws IOException {
        Set<String> userDefinedAliases = schema.getAliases();
        Set<String> allAliases = userDefinedAliases; //could be null
        if (addAliasesForAvro702 && extraAliases != null) {
            allAliases = new HashSet<>();
            if (userDefinedAliases != null) {
                allAliases.addAll(userDefinedAliases);
            }
            for (AvroName extraAlias : extraAliases) {
                allAliases.add(extraAlias.getFull());
            }
        }
        if (allAliases == null || allAliases.isEmpty()) {
            return;
        }
        AvroName name = AvroName.of(schema);
        //"context" namespace for aliases is the fullname of the names type on which they are defined
        //except for the extra alias. scenarios where extraAlias is used are those where the "effective"
        //name of this schema (or its correct form) is different to its full name, so we want
        //to very explicitly use the fullname of the alias for those.
        String referenceNamespace = name.getSpace();
        gen.writeFieldName("aliases");
        gen.writeStartArray();
        //TODO - avro702 may have an impact on (regular) aliases, meaning we may need to
        // add yet more aliases to account for those!
        for (String s : allAliases) {
            AvroName alias = new AvroName(s, null);
            if (addAliasesForAvro702 && extraAliases != null && extraAliases.contains(alias)) {
                //always emit any avro-702 related aliases as fullnames
                gen.writeString(alias.getFull());
            } else {
                String relative = alias.getQualified(referenceNamespace);
                gen.writeString(relative);
            }
        }
        gen.writeEndArray();
    }

    protected void fieldsToJson(
        Schema schema,
        AvroNames names,
        String contextNamespaceWhenParsed,
        String contextNamespaceWhenParsedUnder702,
        G gen
    ) throws IOException {
        gen.writeStartArray();
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field f : fields) {
            gen.writeStartObject();
            gen.writeStringField("name", f.name());
            gen.writeFieldName("type");
            toJson(f.schema(), names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
            if (f.doc() != null) {
                gen.writeStringField("doc", f.doc());
            }
            writeDefaultValue(f, gen);
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

    //returns true if this schema (by fullname) is in names, hence has been written before, and so now
    //just a "ref" (fullname string) will do
    protected boolean writeNameRef(Schema schema, AvroNames names, G gen) throws IOException {
        AvroName name = AvroName.of(schema);
        if (schema.equals(names.get(name))) {
            //"context" namespace depends on which "mode" we're generating in.
            String contextNamespace = preAvro702 ? names.badSpace() : names.correctSpace();
            gen.writeString(name.getQualified(contextNamespace));
            return true;
        }
        if (!name.isAnonymous()) {
            names.put(name, schema); //mark as "seen"
        }
        return false;
    }

    protected Avro702Data writeName(
        Schema schema,
        AvroNames names,
        String contextNamespaceWhenParsed,
        String contextNamespaceWhenParsedUnder702,
        G gen
    ) throws IOException {
        AvroName name = AvroName.of(schema);
        return name.writeName(names, preAvro702, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
    }

    //json generator methods (will vary by jackson version across different avro versions)

    protected abstract G createJsonGenerator(StringWriter writer) throws IOException;

    //properties are very broken across avro versions

    protected abstract boolean hasProps(Schema schema);

    protected abstract void writeProps(Schema schema, G gen) throws IOException;

    protected abstract void writeProps(Schema.Field field, G gen) throws IOException;

    protected abstract void writeDefaultValue(Schema.Field field, G gen) throws IOException;

    protected abstract void writeEnumDefault(Schema enumSchema, G gen) throws IOException;

    protected abstract Set<String> getAliases(Schema.Field field);
}
