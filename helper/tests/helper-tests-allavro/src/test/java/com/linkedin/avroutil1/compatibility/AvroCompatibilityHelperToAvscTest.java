/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests avsc generation on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperToAvscTest {

  public enum SerializationMode {
    TO_AVSC, WRITE_AVSC_WRITER, WRITE_AVSC_STREAM
  }

  @DataProvider(name = "serializationMode")
  public Object[][] serializationMode() {
    return new Object[][]{{SerializationMode.TO_AVSC}, {SerializationMode.WRITE_AVSC_STREAM},
        {SerializationMode.WRITE_AVSC_WRITER}};
  }

  @Test(dataProvider = "serializationMode")
  public void testBadSchemaGeneration(SerializationMode mode) throws Exception {
    //show we can generate the same (bad) schemas that avro 1.4 would under any avro
    testBadSchemaGeneration("avro702/Avro702DemoEnum-good.avsc", "avro702/Avro702DemoEnum-bad.avsc", mode);
    testBadSchemaGeneration("avro702/Avro702DemoFixed-good.avsc", "avro702/Avro702DemoFixed-bad.avsc", mode);
    testBadSchemaGeneration("avro702/Avro702DemoRecord-good.avsc", "avro702/Avro702DemoRecord-bad.avsc", mode);
  }

  @Test(dataProvider = "serializationMode")
  public void testMonsantoSchema(SerializationMode mode) throws Exception {
    String avsc = TestUtil.load("MonsantoRecord.avsc");
    testSchemaRoundtrip(avsc, mode);

    Schema schema = Schema.parse(avsc);
    String badAvsc = serialize(schema, AvscGenerationConfig.LEGACY_PRETTY, mode);
    Schema evilClone = Schema.parse(badAvsc);

    //show avro-702
    Schema correctInnerRecordSchema = schema.getField("outerField").schema().getField("middleField").schema();
    Assert.assertEquals(correctInnerRecordSchema.getNamespace(), "com.acme.outer");
    Schema evilInnerRecordSchema = evilClone.getField("outerField").schema().getField("middleField").schema();
    Assert.assertEquals(evilInnerRecordSchema.getNamespace(), "com.acme.middle");
  }

  @Test(dataProvider = "serializationMode")
  public void testOtherSchemas(SerializationMode mode) throws Exception {
    testSchemaRoundtrip(TestUtil.load("PerfectlyNormalEnum.avsc"), mode);
    testSchemaRoundtrip(TestUtil.load("PerfectlyNormalFixed.avsc"), mode);
    testSchemaRoundtrip(TestUtil.load("PerfectlyNormalRecord.avsc"), mode);
    testSchemaRoundtrip(TestUtil.load("RecordWithDefaults.avsc"), mode);
    testSchemaRoundtrip(TestUtil.load("RecordWithFieldProps.avsc"), mode);
    testSchemaRoundtrip(TestUtil.load("RecordWithLogicalTypes.avsc"), mode);
  }

  @Test(dataProvider = "serializationMode")
  public void testAliasInjectionOnBadSchemas(SerializationMode mode) throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelperCommon.getRuntimeAvroVersion();
    boolean newerThan14 = runtimeVersion.laterThan(AvroVersion.AVRO_1_4);
    //1.11.1 started disrespecting aliases ?!
    boolean is1111 = runtimeVersion.equals(AvroVersion.AVRO_1_11);
    testAliasInjection("avro702/Avro702DemoEnum-good.avsc", "avro702/Avro702DemoEnum-bad.avsc", !newerThan14 || is1111,
        mode);
    testAliasInjection("avro702/Avro702DemoFixed-good.avsc", "avro702/Avro702DemoFixed-bad.avsc", !newerThan14 | is1111,
        mode);
    //even modern avro "tolerates" renaming records ...
    testAliasInjection("avro702/Avro702DemoRecord-good.avsc", "avro702/Avro702DemoRecord-bad.avsc", true, mode);
  }

  private void testAliasInjection(String originalAvscPath, String badAvscPath, boolean vanillaExpectedToWork,
      SerializationMode mode) throws Exception {
    String originalAvsc = TestUtil.load(originalAvscPath);
    Schema originalSchema = Schema.parse(originalAvsc);
    String badAvsc = TestUtil.load(badAvscPath);
    Schema badSchema = Schema.parse(badAvsc);
    long seed = System.currentTimeMillis();

    //demonstrate that bad and original schemas cannot interop by default (except where expected to work)
    RandomRecordGenerator gen = new RandomRecordGenerator();
    //write with good schema, read with bad
    GenericRecord goodRecord =
        (GenericRecord) gen.randomGeneric(originalSchema, RecordGenerationConfig.newConfig().withSeed(seed));
    testBinaryEncodingCycle(goodRecord, badSchema, vanillaExpectedToWork);
    //write with bad schema, read with good
    GenericRecord badRecord =
        (GenericRecord) gen.randomGeneric(badSchema, RecordGenerationConfig.newConfig().withSeed(seed));
    testBinaryEncodingCycle(badRecord, originalSchema, vanillaExpectedToWork);

    //now generate both good and bad schemas with avro-702-mitigation aliases
    String badAvscWithAliases = serialize(originalSchema, AvscGenerationConfig.LEGACY_MITIGATED_PRETTY, mode);
    Schema badSchemaWithAliases = Schema.parse(badAvscWithAliases);
    String goodAvscWithAliases = serialize(originalSchema, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY, mode);
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
  private void testBadSchemaGeneration(String originalAvscPath, String expectedBadAvscPath, SerializationMode mode)
      throws Exception {
    String originalAvsc = TestUtil.load(originalAvscPath);
    String expectedBadAvsc = TestUtil.load(expectedBadAvscPath);
    Schema originalSchema = Schema.parse(originalAvsc);
    Schema expectedBadSchema = Schema.parse(expectedBadAvsc);

    String generatedAvsc = serialize(originalSchema, AvscGenerationConfig.LEGACY_PRETTY, mode);
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
  private void testSchemaRoundtrip(String avsc, SerializationMode mode) throws Exception {
    Schema schema = Schema.parse(avsc);
    String oneLine = serialize(schema, AvscGenerationConfig.CORRECT_ONELINE, mode);
    String pretty = serialize(schema, AvscGenerationConfig.CORRECT_PRETTY, mode);

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
  private void testBinaryEncodingCycle(IndexedRecord record, Schema readerSchema, boolean expectedToWork)
      throws Exception {
    byte[] serialized = AvroCodecUtil.serializeBinary(record);
    try {
      AvroCodecUtil.deserializeAsGeneric(serialized, record.getSchema(), readerSchema);
      //worked.
      Assert.assertTrue(expectedToWork, "was expected to throw");
    } catch (AvroTypeException issue) {
      Assert.assertFalse(expectedToWork, "was expected to succeed");
    }
  }

  private String serialize(Schema schema, AvscGenerationConfig config, SerializationMode mode) {
    switch (mode) {
      case WRITE_AVSC_STREAM:
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AvroCompatibilityHelper.writeAvsc(schema, config, outputStream);
        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      case WRITE_AVSC_WRITER:
        StringWriter writer = new StringWriter();
        AvroCompatibilityHelper.writeAvsc(schema, config, writer);
        return writer.toString();
      default:
        return AvroCompatibilityHelper.toAvsc(schema, config);
    }
  }
}
