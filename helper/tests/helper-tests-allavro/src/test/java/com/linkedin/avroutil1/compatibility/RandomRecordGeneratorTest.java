/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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

  @Test
  public void testRecursiveCollections() throws Exception {
    Schema randomRecord = Schema.parse(TestUtil.load("allavro/RecursiveRecord.avsc"));
    testGeneric(randomRecord);
  }

  @Test
  public void testRecordsWithSelfReferences() throws Exception {
    //default config has a 20% chance of populating a reference to an already-visited schema
    //but these should definitely not throw stack overflows.
    testSpecific(under14.RecordWithSelfReferences.class);
    testGeneric(under14.RecordWithSelfReferences.SCHEMA$);
  }

  private void testSpecific(Class<?> srClass) throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    RecordGenerationConfig config = RecordGenerationConfig.newConfig().withAvoidNulls(true);
    Object srInstance = gen.randomSpecific(srClass, config);
    Assert.assertNotNull(srInstance);
    Assert.assertTrue(srClass.isInstance(srInstance));
  }

  private void testGeneric(Schema schema) throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    RecordGenerationConfig config = RecordGenerationConfig.newConfig().withAvoidNulls(true);
    Object grInstance = gen.randomGeneric(schema, config);
    Assert.assertNotNull(grInstance);
    Assert.assertTrue(grInstance instanceof GenericRecord);
  }
}
