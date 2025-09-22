/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15.codec;

import com.linkedin.avroutil1.compatibility.avro15.backports.SpecificDatumReaderExt;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * an extension of {@link SpecificDatumReader} that, upon failing to look up a class
 * by its fullname (FQCN), also tries fishing for it by aliases. <br>
 *
 * this sort of crap is required when dealing with specific record classes that were generated
 * by avro 1.4 (see AVRO-702)
 *
 * @param <T>
 */
public class AliasAwareSpecificDatumReader<T> extends SpecificDatumReaderExt<T> {

    //same idea as the one in SpecificData
    protected final static Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();

    public AliasAwareSpecificDatumReader() {
        this(null, null);
    }

    public AliasAwareSpecificDatumReader(Class<T> c) {
        this(SpecificData.get().getSchema(c));
    }

    public AliasAwareSpecificDatumReader(Schema schema) {
        this(schema, schema);
    }

    public AliasAwareSpecificDatumReader(Schema writer, Schema reader) {
        super(writer, reader, SpecificData.get());
    }

    @Override
    protected Object newRecord(Object old, Schema schema) {
        Class<?> c = SpecificData.get().getClass(schema); //this is what vanilla does
        if (c == null) {
            c = lookupByAlias(schema);
        }
        if (c == null) {
            dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return (c.isInstance(old) ? old : newInstance(c, schema));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object createEnum(String symbol, Schema schema) {
        //noinspection rawtypes
        Class c = SpecificData.get().getClass(schema);
        if (c == null) {
            c = lookupByAlias(schema);
        }
        if (c == null) {
            dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return Enum.valueOf(c, symbol);
    }

    @Override
    protected Object createFixed(Object old, Schema schema) {
        Class<?> c = SpecificData.get().getClass(schema);
        if (c == null) {
            c = lookupByAlias(schema);
        }
        if (c == null) {
            dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return c.isInstance(old) ? old : newInstance(c, schema);
    }

    protected Class<?> lookupByAlias(Schema namedSchema) {
        Set<String> aliases = namedSchema.getAliases();
        if (aliases != null) {
            for (String alias : aliases) {
                Class<?> byAlias = CLASS_CACHE.computeIfAbsent(alias, fqcn -> {
                    try {
                        return Class.forName(alias);
                    } catch (ClassNotFoundException e) {
                        return null;
                    }
                });
                if (byAlias != null) {
                    return byAlias;
                }
            }
        }
        return null;
    }

    protected void dontPuntToGeneric(Schema schema) {
        //vanilla code "punts to generic" at this point and returns a GenericData$Record.
        //I have never seen this be good for anything except turn this issue into a confusing
        //ClassCastException downstream, so would rather just throw here
        String msg = "unable to find specific record class for schema " + schema.getFullName();
        Set<String> aliases = schema.getAliases();
        if (aliases != null && !aliases.isEmpty()) {
            msg += " (also tried " + aliases.size() + " aliases - " + aliases + ")";
        }
        throw new IllegalStateException(msg);
    }
}
