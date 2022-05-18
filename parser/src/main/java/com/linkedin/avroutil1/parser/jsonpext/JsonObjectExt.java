/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import javax.json.JsonObject;

public interface JsonObjectExt extends JsonObject, JsonStructureExt {

    @Override
    JsonArrayExt getJsonArray(String name);

    @Override
    JsonNumberExt getJsonNumber(String name);

    @Override
    JsonObjectExt getJsonObject(String name);

    @Override
    JsonStringExt getJsonString(String name);

    JsonValueExt get(String name);
}
