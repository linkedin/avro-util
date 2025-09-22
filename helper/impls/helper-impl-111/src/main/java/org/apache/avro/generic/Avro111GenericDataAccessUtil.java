/*
 * Copyright 2025 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package org.apache.avro.generic;

import org.apache.avro.Schema;

/**
 * this class exists to allow us access to package-private classes and methods on class {@link GenericData}
 */
public class Avro111GenericDataAccessUtil {
    private Avro111GenericDataAccessUtil() {
    }

    public static Object getRecordState(GenericData data, Object record, Schema schema) {
        return data.getRecordState(record, schema);
    }

    public static Object getField(GenericData data, Object record, String name, int pos, Object state) {
        return data.getField(record, name, pos, state);
    }

    public static void setField(GenericData data, Object record, String name, int pos, Object value, Object state) {
        data.setField(record, name, pos, value, state);
    }
}
