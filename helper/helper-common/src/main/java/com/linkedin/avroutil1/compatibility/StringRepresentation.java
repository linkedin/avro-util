/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

/**
 * represents the possible ways avro code generation may represent String fields
 * in generated code
 */
public enum StringRepresentation {
    CharSequence, String, Utf8;

    public static StringRepresentation forClass(Class<?> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        //strings are strings
        if (String.class.isAssignableFrom(clazz)) {
            return String;
        }
        //all other CharSequence subclasses are assumed to mean Utf8
        if (CharSequence.class.isAssignableFrom(clazz)) {
            return Utf8;
        }
        throw new IllegalStateException("dont know what to do with a " + clazz.getName());
    }
    public static StringRepresentation forInstance(CharSequence instance) {
        if (instance == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        return forClass(instance.getClass());
    }
}
