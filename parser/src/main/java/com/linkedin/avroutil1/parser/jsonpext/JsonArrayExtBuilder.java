/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import java.util.ArrayList;

public class JsonArrayExtBuilder extends BuilderWithLocations<JsonArrayExt> {
    private ArrayList<JsonValueExt> valueList = new ArrayList<>(1);

    public JsonArrayExtBuilder add(JsonValueExt value) {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        valueList.add(value);
        return this;
    }

    @Override
    public JsonArrayExt build() {
        return new JsonArrayExtImpl(getSource(), getStartLocation(), getEndLocation(), valueList);
    }
}
