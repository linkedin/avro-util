/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

/**
 * represents the possible ways avro code generation may represent String fields
 * in generated java code.
 * specified by setting the "avro.java.string" property on string types in schemas
 */
public enum AvroJavaStringRepresentation {
    CHAR_SEQUENCE, STRING, UTF8;

    public String getJsonValue() {
        switch (this) {
            case CHAR_SEQUENCE:
                return "CharSequence";
            case STRING:
                return "String";
            case UTF8:
                return "Utf8";
        }
        throw new IllegalStateException("unhandled: " + this);
    }

    public static AvroJavaStringRepresentation fromJson(String jsonStringRepStr) {
        if (jsonStringRepStr == null || jsonStringRepStr.isEmpty()) {
            return null;
        }
        switch (jsonStringRepStr) {
            //CharSequence, String, Utf8
            case "CharSequence":
                return CHAR_SEQUENCE;
            case "String":
                return STRING;
            case "Utf8":
                return UTF8;
            default:
                return null;
        }
    }
}
