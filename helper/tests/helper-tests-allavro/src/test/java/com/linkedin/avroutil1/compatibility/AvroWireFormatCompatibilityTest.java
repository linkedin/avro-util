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
import org.apache.avro.io.BinaryDecoder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroWireFormatCompatibilityTest {

  @Test
  public void demonstrateAbleToReadAvro14Binary() throws Exception {
    Schema schema = AvroCompatibilityHelper.parse(TestUtil.load("by14/RecordWithUnion.avsc"));
    byte[] serialized = TestUtil.loadBinary("by14/RecordWithUnion.binary");
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(serialized);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);
    IndexedRecord deserialized = reader.read(null, decoder);
    IndexedRecord inner = (IndexedRecord) deserialized.get(deserialized.getSchema().getField("f").pos());
    Assert.assertEquals(14, inner.get(inner.getSchema().getField("f").pos()));
  }

  @Test
  public void demonstrateAbleToReadAvro15Binary() throws Exception {
    Schema schema = AvroCompatibilityHelper.parse(TestUtil.load("by15/RecordWithUnion.avsc"));
    byte[] serialized = TestUtil.loadBinary("by15/RecordWithUnion.binary");
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(serialized);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);
    IndexedRecord deserialized = reader.read(null, decoder);
    IndexedRecord inner = (IndexedRecord) deserialized.get(deserialized.getSchema().getField("f").pos());
    Assert.assertEquals(15, inner.get(inner.getSchema().getField("f").pos()));
  }
}
