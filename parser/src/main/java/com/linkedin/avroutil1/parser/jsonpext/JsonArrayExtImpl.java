/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.JsonArray;
import jakarta.json.JsonValue;
import jakarta.json.stream.JsonLocation;

import java.nio.file.Path;
import java.util.AbstractList;
import java.util.List;
import java.util.StringJoiner;

public class JsonArrayExtImpl extends AbstractList<JsonValue> implements JsonArrayExt {
    private final Path source;
    private final JsonLocation startLocation;
    private final JsonLocation endLocation;
    private final List<JsonValueExt> valueList;    // Unmodifiable
    private int hashCode;

    JsonArrayExtImpl(
            Path source,
            JsonLocation startLocation,
            JsonLocation endLocation,
            List<JsonValueExt> valueList
    ) {
        this.source = source;
        this.startLocation = startLocation;
        this.endLocation = endLocation;
        this.valueList = valueList;
    }

    @Override
    public Path getSource() {
        return source;
    }

    @Override
    public JsonLocation getStartLocation() {
        return startLocation;
    }

    @Override
    public JsonLocation getEndLocation() {
        return endLocation;
    }

    @Override
    public int size() {
        return valueList.size();
    }

    @Override
    public JsonObjectExt getJsonObject(int index) {
        return (JsonObjectExt) valueList.get(index);
    }

    @Override
    public JsonArrayExt getJsonArray(int index) {
        return (JsonArrayExt) valueList.get(index);
    }

    @Override
    public JsonNumberExt getJsonNumber(int index) {
        return (JsonNumberExt) valueList.get(index);
    }

    @Override
    public JsonStringExt getJsonString(int index) {
        return (JsonStringExt) valueList.get(index);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends JsonValue> List<T> getValuesAs(Class<T> clazz) {
        return (List<T>)valueList;
    }

    @Override
    public String getString(int index) {
        return getJsonString(index).getString();
    }

    @Override
    public String getString(int index, String defaultValue) {
        //TODO - fix index out of bounds
        JsonStringExt jsonString = getJsonString(index);
        return jsonString != null ? jsonString.getString() : defaultValue;
    }

    @Override
    public int getInt(int index) {
        return getJsonNumber(index).intValue();
    }

    @Override
    public int getInt(int index, int defaultValue) {
        JsonNumberExt jsonNumber = getJsonNumber(index);
        try {
            return getInt(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    @Override
    public boolean getBoolean(int index) {
        JsonValue jsonValue = get(index);
        if (jsonValue == null) {
            throw new IllegalStateException();
        }
        ValueType type = jsonValue.getValueType();
        switch (type) {
            case TRUE:
                return true;
            case FALSE:
                return false;
            default:
                throw new ClassCastException("value at index " + index + " is " + type + " and not boolean - " + jsonValue);
        }
    }

    @Override
    public boolean getBoolean(int index, boolean defaultValue) {
        try {
            return getBoolean(index);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    @Override
    public boolean isNull(int index) {
        return valueList.get(index).equals(JsonValue.NULL);
    }

    @Override
    public ValueType getValueType() {
        return ValueType.ARRAY;
    }

    @Override
    public JsonValueExt get(int index) {
        return valueList.get(index);
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = super.hashCode();
        }
        return hashCode;
    }

    @Override
    public JsonArray asJsonArray() {
        return this;
    }

    @Override
    public String toString() {
        StringJoiner csv = new StringJoiner(", ");
        for (JsonValueExt value : valueList) {
            csv.add(value.toString());
        }
        return "[" + csv + "]";
    }
}
