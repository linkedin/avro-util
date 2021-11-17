package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import java.io.IOException;
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
    Assert.assertEquals(fastSpecificDeserializer.getClass().getDeclaredMethods().length, 1);
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
  public void shouldCreateGenericDatumReader() throws IOException, InterruptedException {
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
}
