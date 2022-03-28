/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * represents the possible ways avro code generation may represent String fields
 * in generated java code.
 * specified by setting the "avro.java.string" property on string types in schemas
 */
public enum AvroJavaStringRepresentation {
    CHAR_SEQUENCE("CharSequence"), STRING("String"), UTF8("Utf8");
    private final String jsonValue;

    AvroJavaStringRepresentation(String jsonValue) {
        if (jsonValue == null || jsonValue.isEmpty()) {
            throw new IllegalArgumentException("jsonValue required");
        }
        this.jsonValue = jsonValue;
    }

    private final static List<String> LEGAL_JSON_VALUES;
    static {
        List<String> temp = new ArrayList<>(values().length);
        for (AvroJavaStringRepresentation rep : values()) {
            temp.add(rep.getJsonValue());
        }
        LEGAL_JSON_VALUES = Collections.unmodifiableList(temp);
    }

    public String getJsonValue() {
        return jsonValue;
    }

    public static AvroJavaStringRepresentation fromJson(String jsonStringRepStr) {
        if (jsonStringRepStr == null || jsonStringRepStr.isEmpty()) {
            return null;
        }
        for (AvroJavaStringRepresentation candidate : values()) {
            if (candidate.jsonValue.equals(jsonStringRepStr)) {
                return candidate;
            }
        }
        return null;
    }

    public static List<String> legalJsonValues() {
        return LEGAL_JSON_VALUES;
    }
}
