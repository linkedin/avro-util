/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro15WireFormatCompatibilityTest {

  @Test
  public void demonstrateUnableToReadAvro14Json() throws Exception {
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

  @Test
  public void demonstrateAbleToReadAvro15Json() throws Exception {
    Schema schema = new Schema.Parser().parse(TestUtil.load("by15/RecordWithUnion.avsc"));
    String serialized = TestUtil.load("by15/RecordWithUnion.json");
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, serialized);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);
    IndexedRecord deserialized = reader.read(null, decoder);
    IndexedRecord inner = (IndexedRecord) deserialized.get(deserialized.getSchema().getField("f").pos());
    Assert.assertEquals(15, inner.get(inner.getSchema().getField("f").pos()));
  }
}
