/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import by15.IntRecord;
import by15.LongRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import com.linkedin.avroutil1.Pojo;
import com.linkedin.avroutil1.testcommon.TestUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperAvro15Test {

  @Test
  public void testAvroVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_5;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testAvroCompilerVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_5;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroCompilerVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Avro15SchemaConstructable.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Avro15SchemaConstructable);
    Avro15SchemaConstructable constructable = (Avro15SchemaConstructable)instance;
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
  public void testIntRoundtrip() throws IOException {
    IntRecord intRecord = new IntRecord();
    intRecord.field = 42;
    intRecord.unionField = 55;
    intRecord.arrayField = ImmutableList.of(100, -200);
    intRecord.mapField = ImmutableMap.of("key1", 300, "key2", -400);
    intRecord.unionArrayField = ImmutableList.of(99, -199);
    intRecord.unionMapField = ImmutableMap.of("key1", 298, "key2", 355);
    byte[] binary = toBinary(intRecord);

    IntRecord roundtrip = toSpecificRecord(binary, IntRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(roundtrip.field, 42);
    Assert.assertEquals(roundtrip.unionField.intValue(), 55);
    Assert.assertEquals(roundtrip.arrayField, ImmutableList.of(100, -200));
    Assert.assertEquals(roundtrip.mapField, ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(roundtrip.unionArrayField, ImmutableList.of(99, -199));
    Assert.assertEquals(roundtrip.unionMapField, ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));

    GenericRecord genericRecord = toGenericRecord(binary, IntRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42);
    Assert.assertEquals(genericRecord.get("unionField"), 55);
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("arrayField")), ImmutableList.of(100, -200));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("unionArrayField")), ImmutableList.of(99, -199));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));
  }

  @Test
  public void testLongRoundtrip() throws IOException {
    LongRecord longRecord = new LongRecord();
    longRecord.field = 42L;
    longRecord.unionField = 55L;
    longRecord.arrayField = ImmutableList.of(100L, -200L);
    longRecord.mapField = ImmutableMap.of("key1", 300L, "key2", -400L);
    longRecord.unionArrayField = ImmutableList.of(99L, -199L);
    longRecord.unionMapField = ImmutableMap.of("key1", 298L, "key2", 355L);
    byte[] binary = toBinary(longRecord);

    LongRecord roundtrip = toSpecificRecord(binary, LongRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(roundtrip.field, 42L);
    Assert.assertEquals(roundtrip.unionField.longValue(), 55L);
    Assert.assertEquals(roundtrip.arrayField, ImmutableList.of(100L, -200L));
    Assert.assertEquals(roundtrip.mapField, ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(roundtrip.unionArrayField, ImmutableList.of(99L, -199L));
    Assert.assertEquals(roundtrip.unionMapField, ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));

    GenericRecord genericRecord = toGenericRecord(binary, LongRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42L);
    Assert.assertEquals(genericRecord.get("unionField"), 55L);
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("arrayField")), ImmutableList.of(100L, -200L));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("unionArrayField")), ImmutableList.of(99L, -199L));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));
  }

  @Test
  public void testIntToLongPromotion() throws IOException {
    IntRecord intRecord = new IntRecord();
    intRecord.field = 42;
    intRecord.unionField = 55;
    intRecord.arrayField = ImmutableList.of(100, -200);
    intRecord.mapField = ImmutableMap.of("key1", 300, "key2", -400);
    intRecord.unionArrayField = ImmutableList.of(99, -199);
    intRecord.unionMapField = ImmutableMap.of("key1", 298, "key2", 355);
    byte[] binary = toBinary(intRecord);

    LongRecord longRecord = toSpecificRecord(binary, IntRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(longRecord.field, 42L);
    Assert.assertEquals(longRecord.unionField.longValue(), 55L);
    Assert.assertEquals(longRecord.arrayField, ImmutableList.of(100L, -200L));
    Assert.assertEquals(longRecord.mapField, ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(longRecord.unionArrayField, ImmutableList.of(99L, -199L));
    Assert.assertEquals(longRecord.unionMapField, ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));

    GenericRecord genericRecord = toGenericRecord(binary, IntRecord.SCHEMA$, LongRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42L);
    Assert.assertEquals(genericRecord.get("unionField"), 55L);
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("arrayField")), ImmutableList.of(100L, -200L));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300L, new Utf8("key2"), -400L));
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("unionArrayField")), ImmutableList.of(99L, -199L));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298L, new Utf8("key2"), 355L));
  }

  @Test
  public void testLongToIntDemotion() throws IOException {
    LongRecord longRecord = new LongRecord();
    longRecord.field = 42L;
    longRecord.unionField = 55L;
    longRecord.arrayField = ImmutableList.of(100L, -200L);
    longRecord.mapField = ImmutableMap.of("key1", 300L, "key2", -400L);
    longRecord.unionArrayField = ImmutableList.of(99L, -199L);
    longRecord.unionMapField = ImmutableMap.of("key1", 298L, "key2", 355L);
    byte[] binary = toBinary(longRecord);

    IntRecord intRecord = toSpecificRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(intRecord.field, 42);
    Assert.assertEquals(intRecord.unionField.intValue(), 55);
    Assert.assertEquals(intRecord.arrayField, ImmutableList.of(100, -200));
    Assert.assertEquals(intRecord.mapField, ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(intRecord.unionArrayField, ImmutableList.of(99, -199));
    Assert.assertEquals(intRecord.unionMapField, ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));

    GenericRecord genericRecord = toGenericRecord(binary, LongRecord.SCHEMA$, IntRecord.SCHEMA$);
    Assert.assertEquals(genericRecord.get("field"), 42);
    Assert.assertEquals(genericRecord.get("unionField"), 55);
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("arrayField")), ImmutableList.of(100, -200));
    Assert.assertEquals(genericRecord.get("mapField"), ImmutableMap.of(new Utf8("key1"), 300, new Utf8("key2"), -400));
    Assert.assertEquals(new ArrayList<>((GenericData.Array<Integer>) genericRecord.get("unionArrayField")), ImmutableList.of(99, -199));
    Assert.assertEquals(genericRecord.get("unionMapField"), ImmutableMap.of(new Utf8("key1"), 298, new Utf8("key2"), 355));
  }

  @Test
  public void testLongToIntDemotionOutOfRange() throws IOException {
    LongRecord longRecord = newLongRecord();
    longRecord.field = (long) Integer.MAX_VALUE + 1L;
    byte[] binary = toBinary(longRecord);

    LongRecord longRecord2 = newLongRecord();
    longRecord2.unionField = (long) Integer.MIN_VALUE - 1L;
    byte[] binary2 = toBinary(longRecord2);

    LongRecord longRecord3 = newLongRecord();
    longRecord3.arrayField = ImmutableList.of((long) Integer.MAX_VALUE + 1L);
    byte[] binary3 = toBinary(longRecord3);

    LongRecord longRecord4 = newLongRecord();
    longRecord4.mapField = ImmutableMap.of("haha", (long) Integer.MIN_VALUE - 1L);
    byte[] binary4 = toBinary(longRecord4);

    LongRecord longRecord5 = newLongRecord();
    longRecord5.unionArrayField = ImmutableList.of((long) Integer.MAX_VALUE + 1L);
    byte[] binary5 = toBinary(longRecord5);

    LongRecord longRecord6 = newLongRecord();
    longRecord6.unionMapField = ImmutableMap.of("haha", (long) Integer.MIN_VALUE - 1L);
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

  private LongRecord newLongRecord() {
    LongRecord longRecord = new LongRecord();
    longRecord.field = 0L;
    longRecord.unionField = 0L;
    longRecord.arrayField = ImmutableList.of();
    longRecord.mapField = ImmutableMap.of();
    longRecord.unionArrayField = ImmutableList.of();
    longRecord.unionMapField = ImmutableMap.of();
    return longRecord;
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
