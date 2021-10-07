package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import java.io.IOException;
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

  @Test(groups = {"deserializationTest"})
  @SuppressWarnings("unchecked")
  public void shouldNotCreateFastDeserializerDueToLimit() throws IOException, InterruptedException {
    // Set generatedFastSerDesLimit to 1
    // Try to generate fast deserializer for the first schema
    Schema recordSchema1 = createRecord("TestSchema1", createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
    GenericRecord record1 = new GenericData.Record(recordSchema1);
    record1.put("test", "test");

    FastSerdeCache cacheLimit1 = new FastSerdeCache(Runnable::run, 1);
    FastGenericDatumReader<GenericRecord> fastGenericDatumReader = new FastGenericDatumReader<>(recordSchema1, cacheLimit1);

    // when
    fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record1));

    // then
    FastDeserializer<GenericRecord> fastGenericDeserializer =
        (FastDeserializer<GenericRecord>) cacheLimit1.getFastGenericDeserializer(recordSchema1, recordSchema1);

    fastGenericDeserializer =
        (FastDeserializer<GenericRecord>) cacheLimit1.getFastGenericDeserializer(recordSchema1, recordSchema1);

    Assert.assertNotNull(fastGenericDeserializer);
    Assert.assertNotEquals(1, fastGenericDeserializer.getClass().getDeclaredMethods().length);
    Assert.assertEquals(new Utf8("test"),
        fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record1)).get("test"));

    // Try to generate fast deserializer for the second schema
    // Verify only return FastDeserializerWithAvroGenericImpl
    Schema recordSchema2 = createRecord("TestSchema2", createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
    GenericRecord record2 = new GenericData.Record(recordSchema2);
    record2.put("test", "test");

    // when
    fastGenericDatumReader.read(null, FastSerdeTestsSupport.genericDataAsDecoder(record2));

    // then
    FastDeserializer<GenericRecord> fastGenericDeserializer2 =
        (FastDeserializer<GenericRecord>) cacheLimit1.getFastGenericDeserializer(recordSchema2, recordSchema2);

    fastGenericDeserializer2 =
        (FastDeserializer<GenericRecord>) cacheLimit1.getFastGenericDeserializer(recordSchema2, recordSchema2);

    Assert.assertNotNull(fastGenericDeserializer2);
    Assert.assertEquals(1, fastGenericDeserializer2.getClass().getDeclaredMethods().length);
  }
}
