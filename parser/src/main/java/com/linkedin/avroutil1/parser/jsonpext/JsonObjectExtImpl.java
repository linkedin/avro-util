/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.JsonValue;
import jakarta.json.stream.JsonLocation;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

public class JsonObjectExtImpl extends AbstractMap<String, JsonValue> implements JsonObjectExt {

    private final Path source;
    private final JsonLocation startLocation;
    private final JsonLocation endLocation;
    private final Map<String, JsonValueExt> valueMap;
    private Integer hashCode = null;

    public JsonObjectExtImpl(
            Path source,
            JsonLocation startLocation,
            JsonLocation endLocation,
            Map<String, JsonValueExt> values
    ) {
        this.source = source;
        this.startLocation = startLocation;
        this.endLocation = endLocation;
        this.valueMap = values;
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
    public JsonArrayExt getJsonArray(String name) {
        return (JsonArrayExt) get(name);
    }

    @Override
    public JsonNumberExt getJsonNumber(String name) {
        return (JsonNumberExt) get(name);
    }

    @Override
    public JsonObjectExt getJsonObject(String name) {
        return (JsonObjectExt) get(name);
    }

    @Override
    public JsonStringExt getJsonString(String name) {
        return (JsonStringExt) get(name);
    }

    @Override
    public JsonValueExt get(String name) {
        return (JsonValueExt) super.get(name);
    }

    @Override
    public String getString(String name) {
        return getJsonString(name).getString();
    }

    @Override
    public String getString(String name, String defaultValue) {
        JsonValueExt value = get(name);
        if (value instanceof JsonStringExt) {
            return ((JsonStringExt) value).getString();
        }
        return defaultValue;
    }

    @Override
    public int getInt(String name) {
        return getJsonNumber(name).intValue();
    }

    @Override
    public int getInt(String name, int defaultValue) {
        JsonValueExt value = get(name);
        if (value instanceof JsonNumberExt) {
            return ((JsonNumberExt) value).intValue();
        }
        return defaultValue;
    }

    @Override
    public boolean getBoolean(String name) {
        JsonValueExt value = get(name);
        if (value == null) {
            throw new NullPointerException();
        }
        switch (value.getValueType()) {
            case TRUE:
                return true;
            case FALSE:
                return false;
            default:
                throw new ClassCastException("property " + name + " is a " + value.getValueType() + " and not a boolean");
        }
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        JsonValueExt value = get(name);
        if (value == null) {
            return defaultValue;
        }
        switch (value.getValueType()) {
            case TRUE:
                return true;
            case FALSE:
                return false;
            default:
                return defaultValue;
        }
    }

    @Override
    public boolean isNull(String name) {
        JsonValueExt value = get(name);
        if (value == null) {
            throw new NullPointerException("no such property " + name);
        }
        return value.getValueType() == ValueType.NULL;
    }

    @Override
    public ValueType getValueType() {
        return ValueType.OBJECT;
    }

    @Override
    public int size() {
        return valueMap.size();
    }

    @Override
    public boolean isEmpty() {
        return valueMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return valueMap.containsKey(key);
    }

    @Override
    public JsonValueExt get(Object key) {
        return valueMap.get(key);
    }

    @Override
    public JsonValueExt remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JsonValue put(String key, JsonValue value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends JsonValue> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<String, JsonValue>> entrySet() {
        //we love java generics
        Set<?> raw = valueMap.entrySet();
        //noinspection unchecked
        return (Set<Entry<String, JsonValue>>) raw;
    }

    @Override
    public int hashCode() {
        if (hashCode == null) {
            hashCode = super.hashCode();
        }
        return hashCode;
    }
}
