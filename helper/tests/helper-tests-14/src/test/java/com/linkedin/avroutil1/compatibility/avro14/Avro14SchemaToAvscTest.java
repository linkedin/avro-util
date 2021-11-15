/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Avro14SchemaToAvscTest {

    @Test
    public void demoAvro702() throws Exception {
        String avsc = TestUtil.load("MonsantoRecord.avsc");
        Schema schema = Schema.parse(avsc);
        String badAvsc = schema.toString(true);
        Schema evilClone = Schema.parse(badAvsc);
        Assert.assertNotEquals(evilClone, schema);
        Schema correctInnerRecordSchema = schema.getField("outerField").schema().getField("middleField").schema();
        Assert.assertEquals(correctInnerRecordSchema.getNamespace(), "com.acme.outer");
        Schema evilInnerRecordSchema = evilClone.getField("outerField").schema().getField("middleField").schema();
        Assert.assertEquals(evilInnerRecordSchema.getNamespace(), "com.acme.middle");
    }
}
