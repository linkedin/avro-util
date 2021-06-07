/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import org.apache.avro.AvroRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Avro18BuildersTest {

    @Test
    public void demoAvro19BuildersDontWork() {
        try {
            by19.SimpleRecord.Builder builder = by19.SimpleRecord.newBuilder();
            //the single field in this schema has no default value, and we didnt populate it
            //but the exception it tries to throw doesnt exist under 1.8
            builder.build();
            Assert.fail("expected to throw");
        } catch (NoClassDefFoundError expected) {
            Assert.assertTrue(expected.getMessage().contains("AvroMissingFieldException"));
        }
    }

    @Test
    public void testCompatibleBuildersUnder19() {
        try {
            under19wbuilders.SimpleRecord.Builder builder = under19wbuilders.SimpleRecord.newBuilder();
            //the single field in this schema has no default value, and we didnt populate it
            builder.build();
            Assert.fail("expected to throw");
        } catch (AvroRuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("has no default"));
        }
    }

    @Test
    public void testCompatibleBuildersUnder110() {
        try {
            under110wbuilders.SimpleRecord.Builder builder = under110wbuilders.SimpleRecord.newBuilder();
            //the single field in this schema has no default value, and we didnt populate it
            builder.build();
            Assert.fail("expected to throw");
        } catch (AvroRuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("has no default"));
        }
    }
}
