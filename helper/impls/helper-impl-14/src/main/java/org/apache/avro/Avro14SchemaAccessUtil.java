/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package org.apache.avro;

import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * this class exists to allow us access to package-private classes and methods on class {@link Schema}
 */
public class Avro14SchemaAccessUtil {
    private final static SchemaParseConfiguration SUPPORTED_CONFIG = SchemaParseConfiguration.LOOSE;

    private Avro14SchemaAccessUtil() {
        //util class
    }

    public static SchemaParseResult parse(String schemaJson, Collection<Schema> known) {
        Schema.Names names = new Schema.Names();
        if (known != null) {
            for (Schema s : known) {
                names.add(s);
            }
        }
        Schema parsed = Schema.parse(Schema.parseJson(schemaJson), names);
        Map<String, Schema> allKnown = new HashMap<>(1);
        names.values().forEach(schema -> allKnown.put(schema.getFullName(), schema));
        return new SchemaParseResult(parsed, allKnown, SUPPORTED_CONFIG);
    }
}
