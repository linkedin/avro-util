/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import by17.IntRecord;
import by17.LongRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.avro.util.Utf8;
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
    intRecord.setField(42);
    intRecord.setUnionField(55);
    intRecord.setArrayField(ImmutableList.of(100, -200));
    intRecord.setMapField(ImmutableMap.of("key1", 300, "key2", -400));
    intRecord.setUnionArrayField(ImmutableList.of(99, -199));
    intRecord.setUnionMapField(ImmutableMap.of("key1", 298, "key2", 355));
    byte[] binary = toBinary(intRecord);

    IntRecord roundtrip = toSpecificRecord(binary, IntRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals((int) roundtrip.getField(), 42);
    Assert.assertEquals(roundtrip.getUnionField().intValue(), 55);
    Assert.assertEquals(roundtrip.getArrayField(), ImmutableList.of(100, -200));
    Assert.assertEquals(roundtrip.getMapField(), ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(roundtrip.getUnionArrayField(), ImmutableList.of(99, -199));
    Assert.assertEquals(roundtrip.getUnionMapField(), ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));

    GenericRecord genericRecord = toGenericRecord(binary, IntRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42);
    Assert.assertEquals(genericRecord.get("unionField"), 55);
    Assert.assertEquals(genericRecord.get("arrayField"), ImmutableList.of(100, -200));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(genericRecord.get("unionArrayField"), ImmutableList.of(99, -199));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));
  }

  @Test
  public void testLongRoundtrip() throws IOException {
    LongRecord longRecord = new LongRecord();
    longRecord.setField(42L);
    longRecord.setUnionField(55L);
    longRecord.setArrayField(ImmutableList.of(100L, -200L));
    longRecord.setMapField(ImmutableMap.of("key1", 300L, "key2", -400L));
    longRecord.setUnionArrayField(ImmutableList.of(99L, -199L));
    longRecord.setUnionMapField(ImmutableMap.of("key1", 298L, "key2", 355L));
    byte[] binary = toBinary(longRecord);

    LongRecord roundtrip = toSpecificRecord(binary, LongRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals((long) roundtrip.getField(), 42L);
    Assert.assertEquals(roundtrip.getUnionField().longValue(), 55L);
    Assert.assertEquals(roundtrip.getArrayField(), ImmutableList.of(100L, -200L));
    Assert.assertEquals(roundtrip.getMapField(), ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(roundtrip.getUnionArrayField(), ImmutableList.of(99L, -199L));
    Assert.assertEquals(roundtrip.getUnionMapField(), ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));

    GenericRecord genericRecord = toGenericRecord(binary, LongRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42L);
    Assert.assertEquals(genericRecord.get("unionField"), 55L);
    Assert.assertEquals(genericRecord.get("arrayField"), ImmutableList.of(100L, -200L));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(genericRecord.get("unionArrayField"), ImmutableList.of(99L, -199L));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));
  }

  @Test
  public void testIntToLongPromotion() throws IOException {
    IntRecord intRecord = new IntRecord();
    intRecord.setField(42);
    intRecord.setUnionField(55);
    intRecord.setArrayField(ImmutableList.of(100, -200));
    intRecord.setMapField(ImmutableMap.of("key1", 300, "key2", -400));
    intRecord.setUnionArrayField(ImmutableList.of(99, -199));
    intRecord.setUnionMapField(ImmutableMap.of("key1", 298, "key2", 355));
    byte[] binary = toBinary(intRecord);

    LongRecord longRecord = toSpecificRecord(binary, IntRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals((long) longRecord.getField(), 42L);
    Assert.assertEquals(longRecord.getUnionField().longValue(), 55L);
    Assert.assertEquals(longRecord.getArrayField(), ImmutableList.of(100L, -200L));
    Assert.assertEquals(longRecord.getMapField(), ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(longRecord.getUnionArrayField(), ImmutableList.of(99L, -199L));
    Assert.assertEquals(longRecord.getUnionMapField(), ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));

    GenericRecord genericRecord = toGenericRecord(binary, IntRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42L);
    Assert.assertEquals(genericRecord.get("unionField"), 55L);
    Assert.assertEquals(genericRecord.get("arrayField"), ImmutableList.of(100L, -200L));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(genericRecord.get("unionArrayField"), ImmutableList.of(99L, -199L));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));
  }

  @Test
  public void testLongToIntDemotion() throws IOException {
    LongRecord longRecord = new LongRecord();
    longRecord.setField(42L);
    longRecord.setUnionField(55L);
    longRecord.setArrayField(ImmutableList.of(100L, -200L));
    longRecord.setMapField(ImmutableMap.of("key1", 300L, "key2", -400L));
    longRecord.setUnionArrayField(ImmutableList.of(99L, -199L));
    longRecord.setUnionMapField(ImmutableMap.of("key1", 298L, "key2", 355L));
    byte[] binary = toBinary(longRecord);

    IntRecord intRecord = toSpecificRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals((int) intRecord.getField(), 42);
    Assert.assertEquals(intRecord.getUnionField().intValue(), 55);
    Assert.assertEquals(intRecord.getArrayField(), ImmutableList.of(100, -200));
    Assert.assertEquals(intRecord.getMapField(), ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(intRecord.getUnionArrayField(), ImmutableList.of(99, -199));
    Assert.assertEquals(intRecord.getUnionMapField(), ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));

    GenericRecord genericRecord = toGenericRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42);
    Assert.assertEquals(genericRecord.get("unionField"), 55);
    Assert.assertEquals(genericRecord.get("arrayField"), ImmutableList.of(100, -200));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(genericRecord.get("unionArrayField"), ImmutableList.of(99, -199));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));
  }

  @Test
  public void testLongToIntDemotionOutOfRange() throws IOException {
    LongRecord longRecord = LongRecord.newBuilder().setField((long) Integer.MAX_VALUE + 1L).build();
    byte[] binary = toBinary(longRecord);

    LongRecord longRecord2 = LongRecord.newBuilder().setField(0L).setUnionField((long) Integer.MIN_VALUE - 1L).build();
    byte[] binary2 = toBinary(longRecord2);

    LongRecord longRecord3 = LongRecord.newBuilder().setField(0L).setArrayField(ImmutableList.of((long) Integer.MAX_VALUE + 1L)).build();
    byte[] binary3 = toBinary(longRecord3);

    LongRecord longRecord4 = LongRecord.newBuilder().setField(0L).setMapField(ImmutableMap.of("haha", (long) Integer.MIN_VALUE - 1L)).build();
    byte[] binary4 = toBinary(longRecord4);

    LongRecord longRecord5 = LongRecord.newBuilder().setField(0L).setUnionArrayField(ImmutableList.of((long) Integer.MAX_VALUE + 1L)).build();
    byte[] binary5 = toBinary(longRecord5);

    LongRecord longRecord6 = LongRecord.newBuilder().setField(0L).setUnionMapField(ImmutableMap.of("haha", (long) Integer.MIN_VALUE - 1L)).build();
    byte[] binary6 = toBinary(longRecord6);

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$));

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary2, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary2, LongRecord.SCHEMA$, IntRecord.SCHEMA$));

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary3, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary3, LongRecord.SCHEMA$, IntRecord.SCHEMA$));

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary4, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary4, LongRecord.SCHEMA$, IntRecord.SCHEMA$));

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary5, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary5, LongRecord.SCHEMA$, IntRecord.SCHEMA$));

    Assert.assertThrows(AvroTypeException.class, () -> toSpecificRecord(binary6, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
    Assert.assertThrows(AvroTypeException.class, () -> toGenericRecord(binary6, LongRecord.SCHEMA$, IntRecord.SCHEMA$));
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
