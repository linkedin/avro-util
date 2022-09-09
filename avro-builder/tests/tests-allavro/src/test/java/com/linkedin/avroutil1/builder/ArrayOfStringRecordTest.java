/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.compatibility.AvroCodecUtil;
import com.linkedin.avroutil1.compatibility.RandomRecordGenerator;
import com.linkedin.avroutil1.compatibility.RecordGenerationConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ArrayOfStringRecordTest {

  @Test
  public void testRoundTripSerialization() throws Exception {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs14.ArrayOfStringRecord instance =
        generator.randomSpecific(vs14.ArrayOfStringRecord.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));

    byte[] serialized = AvroCodecUtil.serializeBinary(instance);
    vs14.ArrayOfStringRecord deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs14.ArrayOfStringRecord.getClassSchema(), vs14.ArrayOfStringRecord.class);

    Assert.assertNotSame(deserialized, instance);
    Assert.assertEquals(deserialized, instance);
  }
}
