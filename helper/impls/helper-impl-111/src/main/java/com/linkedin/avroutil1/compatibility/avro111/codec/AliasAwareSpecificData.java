/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */


package com.linkedin.avroutil1.compatibility.avro111.codec;

import com.linkedin.avroutil1.compatibility.ClassLoaderUtil;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AliasAwareSpecificData extends SpecificData {

    //same idea as the one in SpecificData
    protected final static Map<String, Class<?>> CLASS_CACHE = new ConcurrentHashMap<>();
    protected final static Class<?> NO_CLASS = (new Object() {}).getClass();

    private final ClassLoader classLoader;//redefined here because older  1.7 versions dont actually have thisd

    public AliasAwareSpecificData() {
        this.classLoader = null;
    }

    public AliasAwareSpecificData(ClassLoader classLoader) {
        super(classLoader);
        this.classLoader = classLoader;
    }

    @SuppressWarnings("unchecked")
    public Object createEnum(String symbol, Schema schema) {
        @SuppressWarnings("rawtypes")
        Class c = getClass(schema);
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
    public Object createFixed(Object old, Schema schema) {
        Class<?> c = getClass(schema);
        if (c == null) {
            c = lookupByAlias(schema);
        }
        if (c == null) {
            dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return c.isInstance(old) ? old : newInstance(c, schema);
    }

    @Override
    public Object newRecord(Object old, Schema schema) {
        Class<?> c = getClass(schema); //this is what vanilla does
        if (c == null) {
            c = lookupByAlias(schema);
        }
        if (c == null) {
            dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return (c.isInstance(old) ? old : newInstance(c, schema));
    }

    protected Class<?> lookupByAlias(Schema namedSchema) {
        Set<String> aliases = namedSchema.getAliases();
        if (aliases != null) {
            for (String alias : aliases) {
                Class<?> byAlias = CLASS_CACHE.computeIfAbsent(alias, fqcn -> {
                    try {
                        return ClassLoaderUtil.forName(this.classLoader, alias);
                    } catch (ClassNotFoundException e) {
                        return null; //TODO- switch to NO_CLASS
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
