/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.testng.Assert;
import org.testng.annotations.Test;


public class RandomRecordGeneratorTest {

  @Test
  public void testSpecificRecordsWithReservedFieldNames() throws Exception {
    testSpecific(under14.RecordWithReservedFields.class);
    testSpecific(under15.RecordWithReservedFields.class);
    testSpecific(under16.RecordWithReservedFields.class);
    testSpecific(under17.RecordWithReservedFields.class);
    testSpecific(under18.RecordWithReservedFields.class);
    testSpecific(under19.RecordWithReservedFields.class);
    testSpecific(under110.RecordWithReservedFields.class);
    testSpecific(under111.RecordWithReservedFields.class);
  }

  private void testSpecific(Class<?> srClass) throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    RecordGenerationConfig config = RecordGenerationConfig.newConfig().withAvoidNulls(true);
    Object srInstance = gen.randomSpecific(srClass, config);
    Assert.assertNotNull(srInstance);
    Assert.assertTrue(srClass.isInstance(srInstance));
  }
}
