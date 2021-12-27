/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests avsc generation on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperToAvscTest {

  @Test
  public void testMonsantoSchema() throws Exception {
    String avsc = TestUtil.load("MonsantoRecord.avsc");
    testSchema(avsc);

    Schema schema = Schema.parse(avsc);
    String badAvsc = AvroCompatibilityHelper.toAvsc(schema, AvscGenerationConfig.LEGACY_PRETTY);
    Schema evilClone = Schema.parse(badAvsc);

    //show avro-702
    Schema correctInnerRecordSchema = schema.getField("outerField").schema().getField("middleField").schema();
    Assert.assertEquals(correctInnerRecordSchema.getNamespace(), "com.acme.outer");
    Schema evilInnerRecordSchema = evilClone.getField("outerField").schema().getField("middleField").schema();
    Assert.assertEquals(evilInnerRecordSchema.getNamespace(), "com.acme.middle");
  }

  @Test
  public void testOtherSchemas() throws Exception {
    testSchema(TestUtil.load("PerfectlyNormalEnum.avsc"));
    testSchema(TestUtil.load("PerfectlyNormalFixed.avsc"));
    testSchema(TestUtil.load("PerfectlyNormalRecord.avsc"));
    testSchema(TestUtil.load("RecordWithDefaults.avsc"));
    testSchema(TestUtil.load("RecordWithFieldProps.avsc"));
    testSchema(TestUtil.load("RecordWithLogicalTypes.avsc"));
  }

  @Test
  public void testAliasInjectionOnBadSchema() throws Exception {
    long seed = System.currentTimeMillis();
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();

    String originalAvsc = TestUtil.load("avro702/Avro702DemoEnum-good.avsc");
    Schema originalSchema = Schema.parse(originalAvsc);
    String expectedBadAvsc = TestUtil.load("avro702/Avro702DemoEnum-bad.avsc");
    Schema expectedBadSchema = Schema.parse(expectedBadAvsc);

    //demonstrate we can produce the same bad avsc that vanilla 1.4 does
    String badAvsc = AvroCompatibilityHelper.toAvsc(originalSchema, AvscGenerationConfig.LEGACY_PRETTY);
    JsonAssertions.assertThatJson(badAvsc).isEqualTo(expectedBadAvsc);
    Schema badSchema = Schema.parse(badAvsc);
    Assert.assertEquals(badSchema, expectedBadSchema);

    //demonstrate that bad and original schemas cannot interop by default on avro 1.5+
    RandomRecordGenerator gen = new RandomRecordGenerator();
    //write with good schema, read with bad
    GenericRecord goodRecord = (GenericRecord) gen.randomGeneric(originalSchema, RecordGenerationConfig.newConfig().withSeed(seed));
    testBinaryEncodingCycle(goodRecord, badSchema, runtimeVersion.earlierThan(AvroVersion.AVRO_1_5));
    //write with bad schema, read with good
    GenericRecord badRecord = (GenericRecord) gen.randomGeneric(badSchema, RecordGenerationConfig.newConfig().withSeed(seed));
    testBinaryEncodingCycle(badRecord, originalSchema, runtimeVersion.earlierThan(AvroVersion.AVRO_1_5));

    //now generate both good and bad schemas with avro-702-mitigation aliases
    String badAvscWithAliases = AvroCompatibilityHelper.toAvsc(originalSchema, AvscGenerationConfig.LEGACY_MITIGATED_PRETTY);
    Schema badSchemaWithAliases = Schema.parse(badAvscWithAliases);
    String goodAvscWithAliases = AvroCompatibilityHelper.toAvsc(originalSchema, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY);
    Schema goodSchemaWithAliases = Schema.parse(goodAvscWithAliases);
    //and show they can be used to inter-op with their counterparts
    testBinaryEncodingCycle(goodRecord, badSchemaWithAliases, true);
    testBinaryEncodingCycle(badRecord, goodSchemaWithAliases, true);
  }

  /**
   * show that we can "serialize" a schema and parse it back without issues - the the
   * resulting schema is equals() to the original schema
   * @param avsc avsc to run through a parse --> toAvsc --> parse cycle
   * @throws Exception if anything goes wrong
   */
  private void testSchema(String avsc) throws Exception {
    Schema schema = Schema.parse(avsc);
    String oneLine = AvroCompatibilityHelper.toAvsc(schema, AvscGenerationConfig.CORRECT_ONELINE);
    String pretty = AvroCompatibilityHelper.toAvsc(schema, AvscGenerationConfig.CORRECT_PRETTY);

    Schema copy = Schema.parse(oneLine);
    Assert.assertEquals(copy, schema);

    copy = Schema.parse(pretty);
    Assert.assertEquals(copy, schema);
  }

  /**
   * runs an encode + decode cycle on a record and verifies that the results match expectation
   * @param record payload to serialize and decode back. also defines writer schema
   * @param readerSchema schema to decode with
   * @param expectedToWork true if expected to work
   * @throws Exception if anything goes wrong
   */
  private void testBinaryEncodingCycle(IndexedRecord record, Schema readerSchema, boolean expectedToWork) throws Exception {
    byte[] serialized = AvroCodecUtil.serializeBinary(record);
    try {
      AvroCodecUtil.deserializeAsGeneric(serialized, record.getSchema(), readerSchema);
      //worked.
      Assert.assertTrue(expectedToWork, "was expected to throw");
    } catch (AvroTypeException issue) {
      Assert.assertFalse(expectedToWork, "was expected to succeed");
    }
  }
}
