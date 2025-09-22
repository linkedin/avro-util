/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import by17.IntRecord;
import by17.LongRecord;
import com.linkedin.avroutil1.Pojo;
import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperAvro17Test {

  @Test
  public void testAvroVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_7;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testAvroCompilerVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_7;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroCompilerVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Avro17SchemaConstructable.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Avro17SchemaConstructable);
    Avro17SchemaConstructable constructable = (Avro17SchemaConstructable)instance;
    Assert.assertEquals(constructable.getSchema(), schema);
  }

  @Test
  public void testNonSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Pojo.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Pojo);
  }

  @Test
  public void testCreateSchemaFieldWithProvidedDefaultValue() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Schema schema = Schema.parse(TestUtil.load("RecordWithRecursiveTypesAndDefaults.avsc"));
    // Test null default value
    Schema.Field field = schema.getField("unionWithNullDefault");
    Assert.assertTrue(AvroCompatibilityHelper.createSchemaField("unionWithNullDefault", field.schema(), "", null).defaultValue().isNull());
    // Test primitive default value
    field = schema.getField("doubleFieldWithDefault");
    Assert.assertEquals(AvroCompatibilityHelper.createSchemaField("doubleFieldWithDefault", field.schema(), "", 1.0).defaultValue().getDoubleValue(), 1.0);
    // Test map default value
    field = schema.getField("mapOfArrayWithDefault");
    Map<String, List<String>> defaultMapValue = Collections.singletonMap("dummyKey", Collections.singletonList("dummyValue"));
    JsonNode actualJsonNode = AvroCompatibilityHelper.createSchemaField("mapOfArrayWithDefault", field.schema(), "", defaultMapValue).defaultValue();
    Map<String, List<String>> actualMapValue = mapper.convertValue(actualJsonNode, new TypeReference<Map<String, List<String>>>(){});
    Assert.assertEquals(actualMapValue.get("dummyKey").get(0), "dummyValue");
    // Test array default value
    field = schema.getField("arrayOfArrayWithDefault");
    List<List<String>> defaultListValue = Collections.singletonList(Collections.singletonList("dummyElement"));
    actualJsonNode = AvroCompatibilityHelper.createSchemaField("arrayOfArrayWithDefault", field.schema(), "", defaultListValue).defaultValue();
    List<List<String>> actualListValue = mapper.convertValue(actualJsonNode, new TypeReference<List<List<String>>>(){});
    Assert.assertEquals(actualListValue.get(0).get(0), "dummyElement");
  }

  @Test
  public void testGetGenericDefaultValueCloningForEnums() throws IOException {
    // Given a schema with a default enum field
    Schema schema = Schema.parse(TestUtil.load("RecordWithRecursiveTypesAndDefaults.avsc"));
    Schema.Field field = schema.getField("enumFieldWithDefault");

    // When cloning the field with a field builder and setting the default value as the default value from the schema
    Schema.Field clone = AvroCompatibilityHelper.cloneSchemaField(field)
        .setDoc(field.doc())
        .setSchema(field.schema())
        .setDefault(AvroCompatibilityHelper.getGenericDefaultValue(field))
        .build();

    // Then we should expect the clone and original to be equal
    Assert.assertEquals(field, clone);
  }

  @Test
  public void testIntRoundtrip() throws IOException {
    IntRecord intRecord = new IntRecord();
    intRecord.field = 42;
    intRecord.unionField = 55;
    byte[] binary = toBinary(intRecord);

    IntRecord roundtrip = toSpecificRecord(binary, IntRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(roundtrip.field, 42);
    Assert.assertEquals(roundtrip.unionField.intValue(), 55);

    GenericRecord genericRecord = toGenericRecord(binary, IntRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42);
    Assert.assertEquals(genericRecord.get("unionField"), 55);
  }

  @Test
  public void testLongRoundtrip() throws IOException {
    LongRecord longRecord = new LongRecord();
    longRecord.field = 42L;
    longRecord.unionField = 55L;
    byte[] binary = toBinary(longRecord);

    LongRecord roundtrip = toSpecificRecord(binary, LongRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(roundtrip.field, 42L);
    Assert.assertEquals(roundtrip.unionField.longValue(), 55L);

    GenericRecord genericRecord = toGenericRecord(binary, LongRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42L);
    Assert.assertEquals(genericRecord.get("unionField"), 55L);
  }

  @Test
  public void testIntToLongPromotion() throws IOException {
    IntRecord intRecord = new IntRecord();
    intRecord.field = 42;
    intRecord.unionField = 55;
    byte[] binary = toBinary(intRecord);

    LongRecord longRecord = toSpecificRecord(binary, IntRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(longRecord.field, 42L);
    Assert.assertEquals(longRecord.unionField.longValue(), 55L);

    GenericRecord genericRecord = toGenericRecord(binary, IntRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42L);
    Assert.assertEquals(genericRecord.get("unionField"), 55L);
  }

  @Test
  public void testLongToIntDemotion() throws IOException {
    LongRecord longRecord = new LongRecord();
    longRecord.field = 42L;
    longRecord.unionField = 55L;
    byte[] binary = toBinary(longRecord);

    IntRecord intRecord = toSpecificRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(intRecord.field, 42);
    Assert.assertEquals(intRecord.unionField.intValue(), 55);

    GenericRecord genericRecord = toGenericRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42);
    Assert.assertEquals(genericRecord.get("unionField"), 55);
  }

  @Test
  public void testLongToIntDemotionOutOfRange() throws IOException {
    LongRecord longRecord = new LongRecord();
    longRecord.field = (long) Integer.MAX_VALUE + 1L;
    byte[] binary = toBinary(longRecord);

    LongRecord longRecord2 = new LongRecord();
    longRecord2.unionField = (long) Integer.MIN_VALUE - 1L;
    byte[] binary2 = toBinary(longRecord2);

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$));

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary2, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary2, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
  }

  private byte[] toBinary(IndexedRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(baos);
    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(record.getSchema());
    writer.write(record, encoder);
    encoder.flush();
    return baos.toByteArray();
  }

  private <T extends SpecificRecord> T toSpecificRecord(byte[] binary,
                                                        Schema writerSchema,
                                                        Schema readerSchema) throws IOException {
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(binary);
    DatumReader<T> reader = AvroCompatibilityHelper.newSpecificDatumReader(writerSchema, readerSchema, SpecificData.get());
    return reader.read(null, decoder);
  }

  private GenericRecord toGenericRecord(byte[] binary,
                                        Schema writerSchema,
                                        Schema readerSchema) throws IOException {
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(binary);
    DatumReader<GenericRecord> reader = AvroCompatibilityHelper.newGenericDatumReader(writerSchema, readerSchema, GenericData.get());
    return reader.read(null, decoder);
  }
}
