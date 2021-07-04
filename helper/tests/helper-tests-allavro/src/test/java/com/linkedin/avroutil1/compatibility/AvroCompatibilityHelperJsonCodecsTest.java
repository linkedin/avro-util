/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;


/**
 * tests the json encoder/decoder methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperJsonCodecsTest {

  @DataProvider(name = "avroVersions")
  public static Object[] primeNumbers() {
    return new Object[] {"14", "15", "16", "17", "18", "19", "110"};
  }

  @Test(dataProvider = "avroVersions")
  public void testJsonPayloadsWithRecordUnion(String avroVerStr) throws Exception {
    int avroVerInt = Integer.parseInt(avroVerStr);
    AvroVersion avroVerEnum = AvroVersion.valueOf("AVRO_" + avroVerStr.charAt(0) + "_" + avroVerStr.substring(1));
    Schema schema = AvroCompatibilityHelper.parse(TestUtil.load("by" + avroVerStr + "/RecordWithUnion.avsc"));
    String serialized = TestUtil.load("by" + avroVerStr + "/RecordWithUnion.json");

    //test deserialization
    Decoder decoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(schema, serialized);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);
    IndexedRecord deserialized = reader.read(null, decoder);
    IndexedRecord inner = (IndexedRecord) deserialized.get(deserialized.getSchema().getField("f").pos());
    Assert.assertEquals(avroVerInt, inner.get(inner.getSchema().getField("f").pos()));

    //test re-serialization into a the same json format we read is from
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder encoder = (Encoder) AvroCompatibilityHelper.newJsonEncoder(schema, os, true, avroVerEnum);
    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(schema);
    writer.write(deserialized, encoder);
    encoder.flush();

    String reserialized = new String(os.toByteArray(), StandardCharsets.ISO_8859_1);
    JsonAssertions.assertThatJson(reserialized).isEqualTo(serialized);
  }
}
