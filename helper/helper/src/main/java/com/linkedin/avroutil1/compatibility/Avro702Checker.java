/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;

/**
 * utility class that checks if schemas are susceptible to avro-702
 */
public class Avro702Checker {

    private Avro702Checker() {
        //utility class
    }

    public static boolean isSusceptible(Schema schema) {
        String badAvsc = AvroCompatibilityHelper.toBadAvsc(schema, true);
        boolean parseFailed = false;
        Schema evilTwin = null;
        try {
            evilTwin = Schema.parse(badAvsc); //avro-702 can result in "exploded" schemas that dont parse
        } catch (Exception ignored) {
            parseFailed = true;
        }
        return parseFailed || !evilTwin.equals(schema);
    }
}
