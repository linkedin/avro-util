/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */


package com.linkedin.avroutil1.compatibility.avro18.codec;

import com.linkedin.avroutil1.compatibility.codec.ClassCache;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;


public class AliasAwareSpecificData extends SpecificData {

    protected final ClassCache classCache;

    public AliasAwareSpecificData() {
        this.classCache = ClassCache.getDefaultInstance();
    }

    public AliasAwareSpecificData(ClassLoader classLoader) {
        super(classLoader);
        this.classCache = new ClassCache(classLoader);
    }

    @SuppressWarnings("unchecked")
    public Object createEnum(String symbol, Schema schema) {
        @SuppressWarnings("rawtypes")
        Class c = getClass(schema);
        if (c == null) {
            c = classCache.lookupByAlias(schema);
        }
        if (c == null) {
            classCache.dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return Enum.valueOf(c, symbol);
    }

    @Override
    public Object createFixed(Object old, Schema schema) {
        Class<?> c = getClass(schema);
        if (c == null) {
            c = classCache.lookupByAlias(schema);
        }
        if (c == null) {
            classCache.dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return c.isInstance(old) ? old : newInstance(c, schema);
    }

    @Override
    public Object newRecord(Object old, Schema schema) {
        Class<?> c = getClass(schema); //this is what vanilla does
        if (c == null) {
            c = classCache.lookupByAlias(schema);
        }
        if (c == null) {
            classCache.dontPuntToGeneric(schema);
        }
        assert c != null; //make IDE happy
        return (c.isInstance(old) ? old : newInstance(c, schema));
    }
}
