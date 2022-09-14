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


public class DefaultRecordTest {

  @Test
  public void testRoundTripSerializationDefaultRecordValue() throws Exception {
    RandomRecordGenerator generator = new RandomRecordGenerator();
    vs14.RecordDefault instance =
        generator.randomSpecific(vs14.RecordDefault.class, RecordGenerationConfig.newConfig().withAvoidNulls(true));

    byte[] serialized = AvroCodecUtil.serializeBinary(instance);
    vs14.RecordDefault deserialized =
        AvroCodecUtil.deserializeAsSpecific(serialized, vs14.RecordDefault.getClassSchema(), vs14.RecordDefault.class);

    Assert.assertNotSame(deserialized, instance);
    Assert.assertEquals(deserialized, instance);
  }
}
