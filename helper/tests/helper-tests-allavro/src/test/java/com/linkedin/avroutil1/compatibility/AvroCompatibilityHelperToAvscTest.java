/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
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
  public void testBadSchemaGeneration() throws Exception {
    //show we can generate the same (bad) schemas that avro 1.4 would under any avro
    testBadSchemaGeneration("avro702/Avro702DemoEnum-good.avsc", "avro702/Avro702DemoEnum-bad.avsc");
    testBadSchemaGeneration("avro702/Avro702DemoFixed-good.avsc", "avro702/Avro702DemoFixed-bad.avsc");
    testBadSchemaGeneration("avro702/Avro702DemoRecord-good.avsc", "avro702/Avro702DemoRecord-bad.avsc");
  }

  @Test
  public void testMonsantoSchema() throws Exception {
    String avsc = TestUtil.load("MonsantoRecord.avsc");
    testSchemaRoundtrip(avsc);

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
    testSchemaRoundtrip(TestUtil.load("PerfectlyNormalEnum.avsc"));
    testSchemaRoundtrip(TestUtil.load("PerfectlyNormalFixed.avsc"));
    testSchemaRoundtrip(TestUtil.load("PerfectlyNormalRecord.avsc"));
    testSchemaRoundtrip(TestUtil.load("RecordWithDefaults.avsc"));
    testSchemaRoundtrip(TestUtil.load("RecordWithFieldProps.avsc"));
    testSchemaRoundtrip(TestUtil.load("RecordWithLogicalTypes.avsc"));
  }

  @Test
  public void testAliasInjectionOnBadSchemas() throws Exception {
    boolean modernAvro = AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_4);
    testAliasInjection("avro702/Avro702DemoEnum-good.avsc", "avro702/Avro702DemoEnum-bad.avsc", !modernAvro);
    testAliasInjection("avro702/Avro702DemoFixed-good.avsc", "avro702/Avro702DemoFixed-bad.avsc", !modernAvro);
    //even modern avro "tolerates" renaming records ...
    testAliasInjection("avro702/Avro702DemoRecord-good.avsc", "avro702/Avro702DemoRecord-bad.avsc", true);
  }

  private void testAliasInjection(String originalAvscPath, String badAvscPath, boolean vanillaExpectedToWork) throws Exception {
    String originalAvsc = TestUtil.load(originalAvscPath);
    Schema originalSchema = Schema.parse(originalAvsc);
    String badAvsc = TestUtil.load(badAvscPath);
    Schema badSchema = Schema.parse(badAvsc);
    long seed = System.currentTimeMillis();

    //demonstrate that bad and original schemas cannot interop by default (except where expected to work)
    RandomRecordGenerator gen = new RandomRecordGenerator();
    //write with good schema, read with bad
    GenericRecord goodRecord = (GenericRecord) gen.randomGeneric(originalSchema, RecordGenerationConfig.newConfig().withSeed(seed));
    testBinaryEncodingCycle(goodRecord, badSchema, vanillaExpectedToWork);
    //write with bad schema, read with good
    GenericRecord badRecord = (GenericRecord) gen.randomGeneric(badSchema, RecordGenerationConfig.newConfig().withSeed(seed));
    testBinaryEncodingCycle(badRecord, originalSchema, vanillaExpectedToWork);

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
   * given original avsc file and an expected bad avsc file, show that we can generate
   * the bad avsc from the original schema
   * @param originalAvscPath
   * @param expectedBadAvscPath
   * @throws Exception
   */
  private void testBadSchemaGeneration(String originalAvscPath, String expectedBadAvscPath) throws Exception {
    String originalAvsc = TestUtil.load(originalAvscPath);
    String expectedBadAvsc = TestUtil.load(expectedBadAvscPath);
    Schema originalSchema = Schema.parse(originalAvsc);
    Schema expectedBadSchema = Schema.parse(expectedBadAvsc);

    String generatedAvsc = AvroCompatibilityHelper.toAvsc(originalSchema, AvscGenerationConfig.LEGACY_PRETTY);
    Schema parsedSchema = Schema.parse(generatedAvsc);
    Assert.assertEquals(parsedSchema, expectedBadSchema);
    Assert.assertNotEquals(parsedSchema, originalSchema);
  }

  /**
   * show that we can "serialize" a schema correctly and parse it back without issues - the
   * resulting schema is equals() to the original schema
   * @param avsc avsc to run through a parse --> toAvsc --> parse cycle
   * @throws Exception if anything goes wrong
   */
  private void testSchemaRoundtrip(String avsc) throws Exception {
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
