/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * tests the json encoder/decoder methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperJsonCodecsTest {

  @DataProvider(name = "avroVersions")
  public static Object[] primeNumbers() {
    return new Object[] {"14", "15", "16", "17", "18", "19", "110"};
  }

  @Test(dataProvider = "avroVersions")
  public void testDecodeJsonPayloadsWithRecordUnion(String avroVerStr) throws Exception {
    int avroVerInt = Integer.parseInt(avroVerStr);
    Schema schema = AvroCompatibilityHelper.parse(TestUtil.load("by" + avroVerStr + "/RecordWithUnion.avsc"));
    String serialized = TestUtil.load("by" + avroVerStr + "/RecordWithUnion.json");
    Decoder decoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(schema, serialized);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);
    IndexedRecord deserialized = reader.read(null, decoder);
    IndexedRecord inner = (IndexedRecord) deserialized.get(deserialized.getSchema().getField("f").pos());
    Assert.assertEquals(avroVerInt, inner.get(inner.getSchema().getField("f").pos()));
  }
}
