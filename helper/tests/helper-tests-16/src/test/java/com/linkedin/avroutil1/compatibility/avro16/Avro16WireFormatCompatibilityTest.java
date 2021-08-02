/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro16WireFormatCompatibilityTest {

  @Test
  public void demonstrateVanillaUnableToReadAvro14Json() throws Exception {
    Schema schema = new Schema.Parser().parse(TestUtil.load("by14/RecordWithUnion.avsc"));
    String serialized = TestUtil.load("by14/RecordWithUnion.json");
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, serialized);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);
    try {
      reader.read(null, decoder);
      Assert.fail("expected to fail deserialization");
    } catch (AvroTypeException expected) {
      Assert.assertEquals(expected.getMessage(), "Unknown union branch InnerUnionRecord");
    }
  }
}
