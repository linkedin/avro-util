/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.JsonException;

import java.util.LinkedHashMap;

public class JsonObjectExtBuilder extends BuilderWithLocations<JsonObjectExt> {

    protected LinkedHashMap<String, JsonValueExt> valueMap = new LinkedHashMap<>(1);

    JsonObjectExtBuilder add(String name, JsonValueExt value) {
        JsonValueExt conflictingValue = valueMap.get(name);
        if (conflictingValue != null) {
            throw new JsonException("key " + name + " is defined at " + conflictingValue.getStartLocation() + " and again at " + value.getStartLocation());
        }
        valueMap.put(name, value);
        return this;
    }

    public JsonObjectExt build() {
        return new JsonObjectExtImpl(source, startLocation, endLocation, valueMap);
    }
}
