/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.codec;

import com.linkedin.avroutil1.compatibility.ClassLoaderUtil;
import org.apache.avro.Schema;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * looks up specific (generated) classes for avro named schemas by aliases
 */
public class ClassCache {
    //similar to how SpecificData.get() is a "singleton"
    private final static ClassCache DEFAULT_INSTANCE = new ClassCache();

    public static ClassCache getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    /**
     * (optional) classloader to search 1st (or null)
     */
    private final ClassLoader classLoader;

    public ClassCache() {
        this(null);
    }

    public ClassCache(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * this is a cache from fullname (fqcn) to Class instance.
     */
    protected final Map<String, Class<?>> byFqcn = new ConcurrentHashMap<>();

    public Class<?> lookupByAlias(Schema namedSchema) {
        Set<String> aliases = namedSchema.getAliases();
        if (aliases != null) {
            for (String alias : aliases) {
                Class<?> byAlias = byFqcn.computeIfAbsent(alias, fqcn -> {
                    try {
                        return ClassLoaderUtil.forName(classLoader, alias);
                    } catch (ClassNotFoundException e) {
                        //TODO - add negative cache for lookup failures (would need to be at schema level?)
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

    public void dontPuntToGeneric(Schema schema) {
        //vanilla code "punts to generic" if unable to locate a class  and returns a GenericData$Record.
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
