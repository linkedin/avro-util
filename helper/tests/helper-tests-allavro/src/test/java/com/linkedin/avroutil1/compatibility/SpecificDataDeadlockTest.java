/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.testng.Assert;
import org.testng.annotations.Test;
import under111.Outer;


public class SpecificDataDeadlockTest {

    @Test
    public void testNoDeadlock() throws Exception {
        String json = TestUtil.load("allavro/serializedOuter.json");
        SpecificDatumReader<Outer> reader = new SpecificDatumReader<>(Outer.class);
        Decoder decoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(Outer.getClassSchema(), json);
        Outer instance = reader.read(null, decoder);
        Assert.assertNotNull(instance);
    }

}
