package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.InternalRecordField;
import com.linkedin.avro.fastserde.generated.avro.NestedMapInRecordTest;
import com.linkedin.avro.fastserde.generated.avro.NestedMapRecordItem;
import com.linkedin.avro.fastserde.generated.avro.NestedMapTest;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastNestedMapTest {

  @Test
  public void testNestedMapInRecord() throws Exception {
    NestedMapInRecordTest testRecord = new NestedMapInRecordTest();
    Map<String, InternalRecordField> outerMapField = new HashMap<>();
    InternalRecordField mapItemRecord = new InternalRecordField();
    Map<String, NestedMapRecordItem> internalMapField = new HashMap<>();

    String innerKey = "innerKey1";
    String outerKey = "outerKey1";
    Utf8 avroInnerKey = new Utf8(innerKey);
    Utf8 avroOuterKey = new Utf8(outerKey);

    NestedMapRecordItem item = new NestedMapRecordItem();
    FastSerdeTestsSupport.setField(item, "ItemName", "itemName");

    internalMapField.put(innerKey, item);

    FastSerdeTestsSupport.setField(mapItemRecord, "internalMapField", internalMapField);
    FastSerdeTestsSupport.setField(mapItemRecord, "internalNullableLong1", 10L);
    FastSerdeTestsSupport.setField(mapItemRecord, "internalNullableLong2", 20L);
    FastSerdeTestsSupport.setField(mapItemRecord, "internalNullableLong3", 30L);

    outerMapField.put(outerKey, mapItemRecord);

    FastSerdeTestsSupport.setField(testRecord, "mapField", outerMapField);
    FastSerdeTestsSupport.setField(testRecord, "nullableLong1", 40L);

    Decoder decoder = FastSerdeTestsSupport.specificDataAsDecoder(testRecord, NestedMapInRecordTest.SCHEMA$);

    FastSpecificDatumReader<NestedMapInRecordTest> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(NestedMapInRecordTest.SCHEMA$);
    FastDeserializer<NestedMapInRecordTest> fastDeserializer =
        fastSpecificDatumReader.getFastDeserializer().get();

    NestedMapInRecordTest actual = fastDeserializer.deserialize(decoder);

    Map<CharSequence, InternalRecordField> actualNestedMapItems = (Map<CharSequence, InternalRecordField>) FastSerdeTestsSupport.getField(actual, "mapField");
    Assert.assertEquals(1, actualNestedMapItems.size());
    InternalRecordField actualOuterMapFieldRecord = actualNestedMapItems.get(avroOuterKey);
    Map<CharSequence, NestedMapRecordItem> actualInternalMapField = (Map<CharSequence, NestedMapRecordItem>) FastSerdeTestsSupport.getField(actualOuterMapFieldRecord, "internalMapField");
    Assert.assertEquals("itemName", String.valueOf(FastSerdeTestsSupport.getField(actualInternalMapField.get(avroInnerKey),"ItemName")));
    Assert.assertEquals(10L, FastSerdeTestsSupport.getField(actualOuterMapFieldRecord,"internalNullableLong1"));
    Assert.assertEquals(20L, FastSerdeTestsSupport.getField(actualOuterMapFieldRecord,"internalNullableLong2"));
    Assert.assertEquals(30L, FastSerdeTestsSupport.getField(actualOuterMapFieldRecord,"internalNullableLong3"));
    Assert.assertEquals(40L, FastSerdeTestsSupport.getField(actual,"nullableLong1"));
  }

  @Test
  public void testNestedMap() throws Exception {
    NestedMapTest testRecord = new NestedMapTest();
    Map<String, Map<String, Long>> outerMapField = new HashMap<>();
    Map<String, Long> internalMapField = new HashMap<>();

    String innerKey = "innerKey1";
    String outerKey = "outerKey1";
    Utf8 avroInnerKey = new Utf8(innerKey);
    Utf8 avroOuterKey = new Utf8(outerKey);

    internalMapField.put(innerKey, 10L);
    outerMapField.put(outerKey, internalMapField);

    FastSerdeTestsSupport.setField(testRecord, "mapField", outerMapField);

    Decoder decoder = FastSerdeTestsSupport.specificDataAsDecoder(testRecord, NestedMapTest.SCHEMA$);

    FastSpecificDatumReader<NestedMapTest> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(NestedMapTest.SCHEMA$);
    FastDeserializer<NestedMapTest> fastDeserializer =
        fastSpecificDatumReader.getFastDeserializer().get();

    NestedMapTest actual = fastDeserializer.deserialize(decoder);

    Map<CharSequence, Map<CharSequence, Long>> actualNestedMapItems = (Map<CharSequence, Map<CharSequence, Long>>) FastSerdeTestsSupport.getField(actual, "mapField");
    Assert.assertEquals(1, actualNestedMapItems.size());
    Assert.assertEquals(1, actualNestedMapItems.get(avroOuterKey).size());
    Assert.assertEquals(new Long(10L), actualNestedMapItems.get(avroOuterKey).get(avroInnerKey));
  }
}
