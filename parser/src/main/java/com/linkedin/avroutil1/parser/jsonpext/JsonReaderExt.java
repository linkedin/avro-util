/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.jsonpext;

import javax.json.JsonReader;

/**
 * an extended version of {@link javax.json.JsonReader}
 * that returns extended json objects (extensions of the
 * "regular" json-p interfaces)
 */
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
