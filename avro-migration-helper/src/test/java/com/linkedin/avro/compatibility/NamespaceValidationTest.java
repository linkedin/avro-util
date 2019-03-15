/*
 * Copyright 2019 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import com.linkedin.avro.TestUtil;
import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


public class NamespaceValidationTest {

  @Test
  public void testAvro14DoesntValidateNamespace() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (runtimeVersion != AvroVersion.AVRO_1_4) {
      throw new SkipException("only supported under " + AvroVersion.AVRO_1_4 + ". runtime version detected as " + runtimeVersion);
    }
    String withAvsc = TestUtil.load("HasNamespace.avsc");
    Schema with = Schema.parse(withAvsc);
    String withoutAvsc = TestUtil.load("HasNoNamespace.avsc");
    Schema without = Schema.parse(withoutAvsc);

    GenericData.Record record = new GenericData.Record(without);
    record.put("f", AvroCompatibilityHelper.newEnumSymbol(without.getField("f").schema(), "B"));

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    GenericDatumWriter writer = new GenericDatumWriter(without);
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(os);
    //noinspection unchecked
    writer.write(record, encoder);
    encoder.flush();
    byte[] bytes = os.toByteArray();

    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(without, with);
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    GenericData.Record read = reader.read(null, decoder);

    String value = String.valueOf(read.get("f"));
    Assert.assertEquals(value, "B");
  }

  @Test
  public void testModernAvroValidatesNamespaces() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (!runtimeVersion.laterThan(AvroVersion.AVRO_1_4)) {
      throw new SkipException("only supported under modern avro. runtime version detected as " + runtimeVersion);
    }
    String withAvsc = TestUtil.load("HasNamespace.avsc");
    Schema with = Schema.parse(withAvsc);
    String withoutAvsc = TestUtil.load("HasNoNamespace.avsc");
    Schema without = Schema.parse(withoutAvsc);

    GenericData.Record record = new GenericData.Record(without);
    record.put("f", AvroCompatibilityHelper.newEnumSymbol(without.getField("f").schema(), "B"));

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    GenericDatumWriter writer = new GenericDatumWriter(without);
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(os);
    //noinspection unchecked
    writer.write(record, encoder);
    encoder.flush();
    byte[] bytes = os.toByteArray();

    GenericDatumReader<GenericData.Record> reader = new GenericDatumReader<>(without, with);
    BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
    try {
      GenericData.Record read = reader.read(null, decoder);
      Assert.fail("deserialization was expected to fail");
    } catch (Exception expected) {
      Assert.assertTrue(expected.getMessage().contains("Found EnumType, expecting com.acme.EnumType"));
    }
  }
}
