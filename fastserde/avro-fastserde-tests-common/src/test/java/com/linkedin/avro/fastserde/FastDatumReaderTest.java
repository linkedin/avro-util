package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastDatumReaderTest {

  private FastSerdeCache cache;

  @BeforeTest(groups = {"deserializationTest"})
  public void before() {
    cache = new FastSerdeCache(Runnable::run);
  }

  @Test(groups = {"deserializationTest"})
  public void shouldGetFastSpecificDeserializerAndUpdateCachedFastDeserializer() throws Exception {
    // given
    FastSpecificDatumReader<TestRecord> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(TestRecord.SCHEMA$, cache);

    // when
    FastDeserializer<TestRecord> fastSpecificDeserializer =
        fastSpecificDatumReader.getFastDeserializer().get(1, TimeUnit.SECONDS);

    // then
    Assert.assertNotNull(fastSpecificDeserializer);
    Assert.assertTrue(FastSerdeCache.isFastDeserializer(fastSpecificDeserializer));
    Assert.assertTrue(fastSpecificDatumReader.isFastDeserializerUsed());
  }

  @Test(groups = {"deserializationTest"})
  @SuppressWarnings("unchecked")
  public void shouldCreateSpecificDatumReader() throws IOException, InterruptedException {
    // given
    FastSpecificDatumReader<TestRecord> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(TestRecord.SCHEMA$, cache);

    TestRecord testRecord = FastSpecificDeserializerGeneratorTest.emptyTestRecord();
    setField(testRecord, "testEnum", TestEnum.A);

    // when
    fastSpecificDatumReader.read(null, specificDataAsDecoder(testRecord));

    // then
    FastDeserializer<TestRecord> fastSpecificDeserializer =
        (FastDeserializer<TestRecord>) cache.getFastSpecificDeserializer(TestRecord.SCHEMA$, TestRecord.SCHEMA$);

    Assert.assertNotNull(fastSpecificDeserializer);
    Assert.assertNotEquals(2, fastSpecificDeserializer.getClass().getDeclaredMethods().length);
    Assert.assertEquals(TestEnum.A, getField(fastSpecificDatumReader.read(null, specificDataAsDecoder(testRecord)), "testEnum"));
  }

  @Test(groups = {"deserializationTest"})
  @SuppressWarnings("unchecked")
  public void shouldNotCreateSpecificDatumReader() throws IOException, InterruptedException {
    // given
    Schema faultySchema = createRecord("FaultySchema");
    FastSpecificDatumReader<TestRecord> fastSpecificDatumReader =
        new FastSpecificDatumReader<>(TestRecord.SCHEMA$, faultySchema, cache);

    TestRecord testRecord = FastSpecificDeserializerGeneratorTest.emptyTestRecord();
    setField(testRecord, "testEnum", TestEnum.A);

    // when
    fastSpecificDatumReader.read(null, FastSerdeTestsSupport.specificDataAsDecoder(testRecord));

    // then
    FastDeserializer<TestRecord> fastSpecificDeserializer =
        (FastDeserializer<TestRecord>) cache.getFastSpecificDeserializer(TestRecord.SCHEMA$, faultySchema);

    Assert.assertNotNull(fastSpecificDeserializer);
    Assert.assertFalse(fastSpecificDeserializer.isBackedByGeneratedClass());
  }

  @Test(groups = {"deserializationTest"})
  public void shouldGetFastGenericDeserializerAndUpdateCachedFastDeserializer() throws Exception {
    // given
    Schema recordSchema = createRecord("TestSchema", createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
    FastGenericDatumReader<GenericRecord> fastGenericDatumReader = new FastGenericDatumReader<>(recordSchema, cache);

    // when
    FastDeserializer<GenericRecord> fastGenericDeserializer =
        fastGenericDatumReader.getFastDeserializer().get(1, TimeUnit.SECONDS);

    // then
    Assert.assertNotNull(fastGenericDeserializer);
    Assert.assertTrue(FastSerdeCache.isFastDeserializer(fastGenericDeserializer));
    Assert.assertTrue(fastGenericDatumReader.isFastDeserializerUsed());
  }

  @Test(groups = {"deserializationTest"})
  @SuppressWarnings("unchecked")
  public void shouldCreateGenericDatumReader() throws IOException {
    Schema recordSchema = createRecord("TestSchema", createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
    FastGenericDatumReader<GenericRecord> fastGenericDatumReader = new FastGenericDatumReader<>(recordSchema, cache);

    Assert.assertFalse(fastGenericDatumReader.isFastDeserializerUsed(), "FastGenericDatumReader"
        + " shouldn't use the fast deserializer when firstly created");

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("test", "test");

    // when
    fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record));
    Assert.assertFalse(fastGenericDatumReader.isFastDeserializerUsed(), "FastGenericDatumReader shouldn't"
        + " use the fast deserializer during fast class generation");

    // then
    FastDeserializer<GenericRecord> fastGenericDeserializer =
        (FastDeserializer<GenericRecord>) cache.getFastGenericDeserializer(recordSchema, recordSchema);

    Assert.assertNotNull(fastGenericDeserializer);
    Assert.assertNotEquals(2, fastGenericDeserializer.getClass().getDeclaredMethods().length);
    Assert.assertEquals(new Utf8("test"),
        fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record)).get("test"));

    // Block fast class generation
    cache.buildFastGenericDeserializer(recordSchema, recordSchema);
    // Run the de-serialization again
    Assert.assertEquals(new Utf8("test"),
        fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record)).get("test"));
    Assert.assertTrue(fastGenericDatumReader.isFastDeserializerUsed(), "FastGenericDatumReader should be using"
        + " Fast Deserializer when the fast deserializer generation is done.");
  }

  @Test(groups = {"deserializationTest"})
  public void testDatumReaderWithCustomization() throws IOException, ExecutionException, InterruptedException {
    Schema recordSchema = createRecord("TestSchema",
        createField("testInt", Schema.create(Schema.Type.INT)),
        createMapFieldSchema("testMap", Schema.create(Schema.Type.STRING)));
    /**
     * Test with special map type: {@link java.util.concurrent.ConcurrentHashMap}.
     */
    DatumReaderCustomization customization = new DatumReaderCustomization.Builder()
        .setNewMapOverrideFunc( (reuse, size) -> {
          if (reuse instanceof ConcurrentHashMap) {
            ((ConcurrentHashMap)reuse).clear();
            return reuse;
          } else {
            return new ConcurrentHashMap<>(size);
          }
        })
        .build();
    // Check cold datum Reader
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("testInt", new Integer(100));
    Map<Utf8, Utf8> testMap = new HashMap<>();
    testMap.put(new Utf8("key1"), new Utf8("value1"));
    testMap.put(new Utf8("key2"), new Utf8("value2"));
    record.put("testMap", testMap);
    FastGenericDatumReader<GenericRecord> fastGenericDatumReader = new FastGenericDatumReader<>(recordSchema, recordSchema, cache, null, customization);
    GenericRecord deserializedRecordByColdDatumReader = fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record));
    Assert.assertEquals(deserializedRecordByColdDatumReader.get("testInt"), new Integer(100));
    Assert.assertEquals(deserializedRecordByColdDatumReader.get("testMap"), testMap);
    Assert.assertTrue(deserializedRecordByColdDatumReader.get("testMap") instanceof ConcurrentHashMap);

    // Block the fast deserializer generation
    fastGenericDatumReader.getFastDeserializer().get();
    // Decode the record by fast datum reader
    GenericRecord deserializedRecordByFastDatumReader = fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record));
    Assert.assertEquals(deserializedRecordByFastDatumReader.get("testInt"), new Integer(100));
    Assert.assertEquals(deserializedRecordByFastDatumReader.get("testMap"), testMap);
    Assert.assertTrue(deserializedRecordByFastDatumReader.get("testMap") instanceof ConcurrentHashMap);

    // Test with an empty map
    GenericRecord recordWithEmptyMap = new GenericData.Record(recordSchema);
    recordWithEmptyMap.put("testInt", new Integer(200));
    recordWithEmptyMap.put("testMap", Collections.emptyMap());
    GenericRecord deserializedRecordWithEmptyMapByFastDatumReader = fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(recordWithEmptyMap));
    Assert.assertEquals(deserializedRecordWithEmptyMapByFastDatumReader.get("testInt"), new Integer(200));
    Assert.assertEquals(deserializedRecordWithEmptyMapByFastDatumReader.get("testMap"), Collections.emptyMap());
    Assert.assertTrue(deserializedRecordWithEmptyMapByFastDatumReader.get("testMap") instanceof ConcurrentHashMap);

    // Generate a new fast datum reader with the same schema, but without customization
    FastGenericDatumReader<GenericRecord> fastGenericDatumReaderWithoutCustomization = new FastGenericDatumReader<>(recordSchema, cache);
    GenericRecord deserializedRecordByFastDatumReaderWithoutCustomization = fastGenericDatumReaderWithoutCustomization.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record));
    Assert.assertEquals(deserializedRecordByFastDatumReaderWithoutCustomization.get("testInt"), new Integer(100));
    Assert.assertEquals(deserializedRecordByFastDatumReaderWithoutCustomization.get("testMap"), testMap);
    Assert.assertFalse(deserializedRecordByFastDatumReaderWithoutCustomization.get("testMap") instanceof ConcurrentHashMap);
  }
}
