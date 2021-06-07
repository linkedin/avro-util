/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import org.apache.avro.AvroMissingFieldException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Avro110BuildersTest {

    @Test (expectedExceptions = AvroMissingFieldException.class)
    public void demoDefaultValueExceptionOnBuild() {
        by110.SimpleRecord.Builder builder = by110.SimpleRecord.newBuilder();
        //the single field in this schema has no default value, and we didnt populate it
        builder.build();
    }

    @Test
    public void testCompatibleBuildersUnder19() {
        try {
            under19wbuilders.SimpleRecord.Builder builder = under19wbuilders.SimpleRecord.newBuilder();
            //the single field in this schema has no default value, and we didnt populate it
            builder.build();
            Assert.fail("expected to throw");
        } catch (AvroMissingFieldException expected) {
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
        } catch (AvroMissingFieldException expected) {
            Assert.assertTrue(expected.getMessage().contains("has no default"));
        }
    }
}
