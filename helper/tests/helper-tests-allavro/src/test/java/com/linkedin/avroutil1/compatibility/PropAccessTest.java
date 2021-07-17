/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PropAccessTest {

    @Test
    public void testSchemaStringPropAccess() throws Exception {
        String avsc = TestUtil.load("RecordWithFieldProps.avsc");
        Schema schema = Schema.parse(avsc);
        Schema.Field originalField = schema.getField("stringField");

        //show that can access string props under all avro

        Assert.assertEquals(originalField.getProp("stringProp"), "stringValue");

        originalField.addProp("moreStringProp", "moreStringValue");
        Assert.assertEquals(originalField.getProp("moreStringProp"), "moreStringValue");
    }
}
