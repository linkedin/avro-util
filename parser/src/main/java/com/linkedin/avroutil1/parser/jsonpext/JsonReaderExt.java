/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import jakarta.json.JsonReader;

public interface JsonReaderExt extends JsonReader {

    @Override
    JsonStructureExt read();

    @Override
    JsonObjectExt readObject();

    @Override
    JsonArrayExt readArray();

    @Override
    JsonValueExt readValue();
}
