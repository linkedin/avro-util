/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.compatibility.backports.Avro702Data;
import com.linkedin.avroutil1.compatibility.backports.AvroName;
import com.linkedin.avroutil1.compatibility.backports.AvroNames;
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AvscWriter<G extends JsonGeneratorWrapper<?>> {
    protected final List<AvscWriterPlugin> _plugins;

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

    protected final boolean retainNonClaimedProps;
    protected final boolean retainFieldAliases;
    protected final boolean retainSchemaAliases;
    protected final boolean retainDefaults;
    protected final boolean retainDocs;

    protected AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702) {
        this.pretty = pretty;
        this.preAvro702 = preAvro702;
        this.addAliasesForAvro702 = addAliasesForAvro702;
        this.retainDocs = true;
        this.retainSchemaAliases = true;
        this.retainFieldAliases = true;
        this.retainNonClaimedProps = true;
        this.retainDefaults = true;
        _plugins = new ArrayList<AvscWriterPlugin>(0);
    }

    protected AvscWriter(boolean pretty, boolean preAvro702, boolean addAliasesForAvro702, boolean retainDefaults,
        boolean retainDocs, boolean retainFieldAliases, boolean retainNonClaimedProps, boolean retainSchemaAliases,
        List<AvscWriterPlugin> plugins) {
        this.pretty = pretty;
        this.preAvro702 = preAvro702;
        this.addAliasesForAvro702 = addAliasesForAvro702;
        this.retainDocs = retainDocs;
        this.retainSchemaAliases = retainSchemaAliases;
        this.retainFieldAliases = retainFieldAliases;
        this.retainNonClaimedProps = retainNonClaimedProps;
        this.retainDefaults = retainDefaults;
        this._plugins = plugins == null ? Collections.emptyList() : plugins;
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
            e.printStackTrace();
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
                avro702Data = writeName(schema, names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                gen.writeStringField("type", "enum");
                gen.writeArrayFieldStart("symbols");
                for (String symbol : schema.getEnumSymbols()) {
                    gen.writeString(symbol);
                }
                gen.writeEndArray();
                if(retainDefaults) {
                    writeEnumDefault(schema, gen);
                }
                Set<String> claimedPropsEnum = executeAvscWriterPluginsForSchema(schema, gen);

                if(retainNonClaimedProps) {
                    writeProps(schema, gen, claimedPropsEnum);
                }

                aliasesToJson(schema, avro702Data.getExtraAliases(), gen, retainSchemaAliases);
                if (retainDocs && schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                gen.writeEndObject();
                break;
            case FIXED:
                //taken from FixedSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                gen.writeStartObject();
                avro702Data = writeName(schema, names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                gen.writeStringField("type", "fixed");
                gen.writeNumberField("size", schema.getFixedSize());

                Set<String> claimedPropsFixed = executeAvscWriterPluginsForSchema(schema, gen);

                if(retainNonClaimedProps) {
                    writeProps(schema, gen, claimedPropsFixed);
                }

                aliasesToJson(schema, avro702Data.getExtraAliases(), gen, retainSchemaAliases);
                if (retainDocs && schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
                gen.writeEndObject();
                break;
            case RECORD:
                //taken from RecordSchema.toJson() in avro 1.11
                if (writeNameRef(schema, names, gen)) {
                    return;
                }
                gen.writeStartObject();
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
                gen.writeStringField("type", schema.isError() ? "error" : "record");
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
                Set<String> claimedPropsRecord = executeAvscWriterPluginsForSchema(schema, gen);

                if(retainNonClaimedProps) {
                    writeProps(schema, gen, claimedPropsRecord);
                }

                aliasesToJson(schema, avro702Data.getExtraAliases(), gen, retainSchemaAliases);
                if (retainDocs && schema.getDoc() != null) {
                    gen.writeStringField("doc", schema.getDoc());
                }
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
                Set<String> claimedPropsArray = executeAvscWriterPluginsForSchema(schema, gen);

                if(retainNonClaimedProps) {
                    writeProps(schema, gen, claimedPropsArray);
                }
                gen.writeEndObject();
                break;
            case MAP:
                //taken from MapSchema.toJson() in avro 1.11
                gen.writeStartObject();
                gen.writeStringField("type", "map");
                gen.writeFieldName("values");
                toJson(schema.getValueType(), names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                Set<String> claimedPropsMap = executeAvscWriterPluginsForSchema(schema, gen);

                if(retainNonClaimedProps) {
                    writeProps(schema, gen, claimedPropsMap);
                }
                gen.writeEndObject();
                break;
            case UNION:
                //taken from UnionSchema.toJson() in avro 1.11
                gen.writeStartArray();
                for (Schema type : schema.getTypes()) {
                    toJson(type, names, contextNamespaceWhenParsed, contextNamespaceWhenParsedUnder702, gen);
                }
                Set<String> claimedPropsUnion = executeAvscWriterPluginsForSchema(schema, gen);
                if(retainNonClaimedProps) {
                    writeProps(schema, gen, claimedPropsUnion);
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
                    Set<String> claimedProps = executeAvscWriterPluginsForSchema(schema, gen);

                    if(retainNonClaimedProps) {
                        writeProps(schema, gen, claimedProps);
                    }
                    gen.writeEndObject();
                }
        }
    }

    /***
     * Runs execute(Schema, JsonGeneratorWrapper) for provided AvscWriterPlugins
     * @param schema
     * @param gen
     * @return list of Json prop names handled by plugins
     * @throws UnsupportedOperationException of 2+ plugins provided for same json prop
     */
    private Set<String> executeAvscWriterPluginsForSchema(Schema schema, G gen) {
        Set<String> claimedProps = new HashSet<>();
        for(AvscWriterPlugin plugin : _plugins) {
            String propName = plugin.execute(schema, gen);
            if(propName!= null && !propName.isEmpty()) {
                if(claimedProps.contains(propName)) {
                    throw new UnsupportedOperationException("Only 1 AvscWriterPlugin per Json property allowed.");
                }
                claimedProps.add(propName);
            }
        }
        return claimedProps;
    }

    /***
     * Runs execute(Field, JsonGeneratorWrapper) for provided AvscWriterPlugins
     * @param field
     * @param gen
     * @return list of Json prop names handled by plugins
     * @throws UnsupportedOperationException of 2+ plugins provided for same json prop
     */
    private Set<String> executeAvscWriterPluginsForField(Schema.Field field, G gen) {
        Set<String> claimedProps = new HashSet<>();
        for(AvscWriterPlugin plugin : _plugins) {
            String propName = plugin.execute(field, gen);
            if(propName!= null && !propName.isEmpty()) {
                if(claimedProps.contains(propName)) {
                    throw new UnsupportedOperationException("Only 1 AvscWriterPlugin per Json property allowed.");
                }
                claimedProps.add(propName);
            }
        }
        return claimedProps;
    }

    protected void aliasesToJson(Schema schema, List<AvroName> extraAliases, G gen, boolean retainSchemaAliases) throws IOException {
        Set<String> userDefinedAliases =
            retainSchemaAliases ? getSortedFullyQualifiedSchemaAliases(schema.getAliases(), schema.getNamespace()) : null;
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

    /***
     * Checks if each alias in aliases has a namespace.name, adds parent namespace if not present
     * @param aliases
     * @param namespace
     * @return sorted fully qualified aliases set
     */
    private Set<String> getSortedFullyQualifiedSchemaAliases(Set<String> aliases, String namespace) {
        if(aliases == null) return null;
        Set<String> sortedAliases = new TreeSet<>();
        for(String alias : aliases) {
            if(alias.contains(namespace)) {
                sortedAliases.add(alias);
            } else {
                sortedAliases.add(namespace + "." + alias);
            }
        }
        return sortedAliases;
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
            if (retainDocs && f.doc() != null) {
                gen.writeStringField("doc", f.doc());
            }
            if(retainDefaults) {
                writeDefaultValue(f, gen);
            }
            if (f.order() != Schema.Field.Order.ASCENDING) {
                gen.writeStringField("order", f.order().name());
            }
            if(retainFieldAliases) {
                Set<String> aliases = getAliases(f);
                if (aliases != null && aliases.size() != 0) {
                    Set<String> sortedAliases = new TreeSet<>(aliases);
                    gen.writeFieldName("aliases");
                    gen.writeStartArray();
                    for (String alias : sortedAliases) {
                        gen.writeString(alias);
                    }
                    gen.writeEndArray();
                }
            }
            Set<String> claimedProps = executeAvscWriterPluginsForField(f, gen);
            if(retainNonClaimedProps) {
                writeProps(f, gen, claimedProps);
            }
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

    /***
     * Write all json props from schema, except the keys provided in claimedProps
     * @param schema
     * @param gen
     * @param claimedProps
     * @throws IOException
     */
    protected abstract void writeProps(Schema schema, G gen, Set<String> claimedProps) throws IOException;

    /***
     * Write all json props from field, except the keys provided in claimedProps
     * @param field
     * @param gen
     * @param claimedProps
     * @throws IOException
     */
    protected abstract void writeProps(Schema.Field field, G gen, Set<String> claimedProps) throws IOException;

    protected abstract void writeDefaultValue(Schema.Field field, G gen) throws IOException;

    protected abstract void writeEnumDefault(Schema enumSchema, G gen) throws IOException;

    protected abstract Set<String> getAliases(Schema.Field field);
}
