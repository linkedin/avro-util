/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroRecordUtilTest {

  @Test
  public void testSupplementDefaultsIntoGenericRecord() throws Exception {
    String avsc = TestUtil.load("RecordWithDefaults.avsc");
    Schema schema = Schema.parse(avsc);

    GenericData.Record record = new GenericData.Record(schema);

    //there are unpopulated fields with no defaults, see that full population is thus impossible
    Assert.assertThrows(IllegalArgumentException.class, () -> AvroRecordUtil.supplementDefaults(record, true));

    //populate only those missing fields that have defaults
    AvroRecordUtil.supplementDefaults(record, false);

    //still missing values
    Assert.assertThrows(() -> AvroCodecUtil.serializeBinary(record));

    //provide values for (ONLY) those fields that dont have defaults in the schema
    record.put("boolWithoutDefault", true); //this starts out null
    record.put("strWithoutDefault", "I liek milk");

    //should now pass
    AvroCodecUtil.serializeBinary(record);
  }

  @Test
  public void testSupplementDefaultIntoSpecificRecord() throws Exception {
    under14.RecordWithDefaults record = new under14.RecordWithDefaults();

    //there are unpopulated fields with no defaults, see that full population is thus impossible
    Assert.assertThrows(IllegalArgumentException.class, () -> AvroRecordUtil.supplementDefaults(record, true));

    //populate only those missing fields that have defaults
    AvroRecordUtil.supplementDefaults(record, false);

    //still missing values
    Assert.assertThrows(() -> AvroCodecUtil.serializeBinary(record));

    //provide values for (ONLY) those fields that dont have defaults in the schema
    record.strWithoutDefault = "I liek milk";

    //should now pass
    AvroCodecUtil.serializeBinary(record);
  }

  @Test
  public void testTrivialGenericToSpecificConversion() throws Exception {
    Schema schema = under111.SimpleRecord.SCHEMA$;
    RandomRecordGenerator gen = new RandomRecordGenerator();
    GenericRecord genericInstance = (GenericRecord) gen.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    convertRoundTrip(genericInstance);
  }

  @Test
  public void testGenericToSpecific() throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    Schema schema;
    GenericRecord genericInstance;

    schema = under14.RecordWithDefaults.SCHEMA$;
    genericInstance = (GenericRecord) gen.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(false));
    convertRoundTrip(genericInstance);

    schema = under14.HasComplexDefaults.SCHEMA$;
    genericInstance = (GenericRecord) gen.randomGeneric(schema, RecordGenerationConfig.newConfig().withAvoidNulls(true));
    convertRoundTrip(genericInstance);
  }

  @Test
  public void testGenericToSpecificComplexCollections() throws Exception {
    RandomRecordGenerator gen = new RandomRecordGenerator();
    RecordGenerationConfig genConfig = RecordGenerationConfig.newConfig().withAvoidNulls(true);

    Schema schema = under14.RecordWithCollectionsOfUnions.SCHEMA$;
    GenericRecord genericInstance = (GenericRecord) gen.randomGeneric(schema, genConfig);
    convertRoundTrip(genericInstance);
  }

  private void convertRoundTrip(GenericRecord original) {
    Assert.assertNotNull(original);
    SpecificRecord converted = AvroRecordUtil.genericRecordToSpecificRecord(original, null, RecordConversionConfig.ALLOW_ALL);
    Assert.assertNotNull(converted);
    GenericRecord backAgain = AvroRecordUtil.specificRecordToGenericRecord(converted, null, RecordConversionConfig.ALLOW_ALL);
    Assert.assertNotSame(original, backAgain);
    try {
      Assert.assertEquals(backAgain, original);
    } catch (AvroRuntimeException expected) {
      //avro 1.4 cant compare anything with map schemas for equality
      if (!expected.getMessage().contains("compare maps") || AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_4)) {
        Assert.fail("while attempting to compare generic records", expected);
      }
    }
  }
}
