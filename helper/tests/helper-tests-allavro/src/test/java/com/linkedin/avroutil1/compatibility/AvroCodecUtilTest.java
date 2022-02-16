/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;
import under14.RecordWithAllTypes;


public class AvroCodecUtilTest {

  @Test
  public void testSpecificRoundTrip() throws Exception {
    skipUnderOldAvro();
    RandomRecordGenerator gen = new RandomRecordGenerator();
    RecordGenerationConfig config = RecordGenerationConfig.newConfig().withAvoidNulls(true);
    RecordWithAllTypes record = gen.randomSpecific(RecordWithAllTypes.class, config);

    byte[] binary = AvroCodecUtil.serializeBinary(record);
    String json = AvroCodecUtil.serializeJson(record, AvroCompatibilityHelper.getRuntimeAvroVersion());

    RecordWithAllTypes deserializedFromBinary = AvroCodecUtil.deserializeAsSpecific(
        binary, record.getSchema(), record.getClass());

    Assert.assertEquals(deserializedFromBinary, record);

    RecordWithAllTypes deserializedFromJson = AvroCodecUtil.deserializeAsSpecific(
        json, record.getSchema(), record.getClass());

    Assert.assertEquals(deserializedFromJson, record);
  }

  @Test
  public void testGenericRoundTrip() throws Exception {
    skipUnderOldAvro();
    RandomRecordGenerator gen = new RandomRecordGenerator();
    RecordGenerationConfig config = RecordGenerationConfig.newConfig().withAvoidNulls(true);
    GenericRecord record = (GenericRecord) gen.randomGeneric(RecordWithAllTypes.getClassSchema(), config);

    byte[] binary = AvroCodecUtil.serializeBinary(record);
    String json = AvroCodecUtil.serializeJson(record, AvroCompatibilityHelper.getRuntimeAvroVersion());

    GenericRecord deserializedFromBinary = AvroCodecUtil.deserializeAsGeneric(
        binary, record.getSchema(), record.getSchema());

    Assert.assertEquals(deserializedFromBinary, record);

    GenericRecord deserializedFromJson = AvroCodecUtil.deserializeAsGeneric(
        json, record.getSchema(), record.getSchema());

    Assert.assertEquals(deserializedFromJson, record);
  }

  private void skipUnderOldAvro() {
    AvroVersion avroVer = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (avroVer.earlierThan(AvroVersion.AVRO_1_8)) {
      //in reality avro gains this ability mid-1.7, but our detection is too coarse for this
      throw new SkipException("avro " + avroVer + " cant compare records with map fields");
    }
  }
}
