/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.JsonDecoder;
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

  @Test
  public void demonstrateVanillaAvroJsonParsingOfWrongNumericLiterals() throws Exception {
    Schema schema = AvroCompatibilityHelper.parse(TestUtil.load("allavro/RecordWithNumericFields.avsc"));
    String correctJson = TestUtil.load("allavro/RecordWithNumericFields-correct.json");
    String wrongJson = TestUtil.load("allavro/RecordWithNumericFields-wrong.json");

    JsonDecoder correctVanillaDecoder = AvroCompatibilityHelper.newJsonDecoder(schema, correctJson);
    JsonDecoder wrongVanillaDecoder = AvroCompatibilityHelper.newJsonDecoder(schema, wrongJson);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);

    //should always succeed
    IndexedRecord deserialized1 = reader.read(null, correctVanillaDecoder);
    Assert.assertEquals(deserialized1.get(schema.getField("intField").pos()), 1);
    Assert.assertEquals(deserialized1.get(schema.getField("longField").pos()), 2L);
    Assert.assertEquals(deserialized1.get(schema.getField("floatField").pos()), 3.0f);
    Assert.assertEquals(deserialized1.get(schema.getField("doubleField").pos()), 4.0d);

    AvroVersion avroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    try {
      IndexedRecord deserialized2 = reader.read(null, wrongVanillaDecoder);
      //succeeds under 1.7+
      Assert.assertTrue(avroVersion.laterThan(AvroVersion.AVRO_1_6), "parsing of wrong json numeral literals expected to fail under " + avroVersion);

      Assert.assertEquals(deserialized2.get(schema.getField("intField").pos()), 1);
      Assert.assertEquals(deserialized2.get(schema.getField("longField").pos()), 2L);
      Assert.assertEquals(deserialized2.get(schema.getField("floatField").pos()), 3.0f);
      Assert.assertEquals(deserialized2.get(schema.getField("doubleField").pos()), 4.0d);
    } catch (AvroTypeException expected) {
      //fails under < 1.7
      Assert.assertTrue(avroVersion.earlierThan(AvroVersion.AVRO_1_7), "parsing of wrong json numeral literals expected to succeed under " + avroVersion);
    }
  }

  @Test
  public void testCompatibleJsonParsingOfWrongNumericLiterals() throws Exception {
    Schema schema = AvroCompatibilityHelper.parse(TestUtil.load("allavro/RecordWithNumericFields.avsc"));
    String correctJson = TestUtil.load("allavro/RecordWithNumericFields-correct.json");
    String wrongJson = TestUtil.load("allavro/RecordWithNumericFields-wrong.json");

    Decoder correctCompatibleDecoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(schema, correctJson);
    Decoder wrongCompatibleDecoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(schema, wrongJson);
    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema);

    IndexedRecord deserialized1 = reader.read(null, correctCompatibleDecoder);
    IndexedRecord deserialized2 = reader.read(null, wrongCompatibleDecoder);

    Assert.assertEquals(deserialized1.get(schema.getField("intField").pos()), 1);
    Assert.assertEquals(deserialized1.get(schema.getField("longField").pos()), 2L);
    Assert.assertEquals(deserialized1.get(schema.getField("floatField").pos()), 3.0f);
    Assert.assertEquals(deserialized1.get(schema.getField("doubleField").pos()), 4.0d);

    Assert.assertEquals(deserialized2.get(schema.getField("intField").pos()), 1);
    Assert.assertEquals(deserialized2.get(schema.getField("longField").pos()), 2L);
    Assert.assertEquals(deserialized2.get(schema.getField("floatField").pos()), 3.0f);
    Assert.assertEquals(deserialized2.get(schema.getField("doubleField").pos()), 4.0d);
  }
}
