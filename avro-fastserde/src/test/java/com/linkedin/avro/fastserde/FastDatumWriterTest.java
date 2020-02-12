package com.linkedin.avro.fastserde;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastDatumWriterTest {

  private FastSerdeCache cache;

  @BeforeTest(groups = {"serializationTest"})
  public void before() {
    cache = new FastSerdeCache(Runnable::run);
  }

  @Test(groups = {"serializationTest"})
  @SuppressWarnings("unchecked")
  public void shouldCreateSpecificDatumWriter() throws IOException, InterruptedException {
    // given
    FastSpecificDatumWriter<TestRecord> fastSpecificDatumWriter =
        new FastSpecificDatumWriter<>(TestRecord.SCHEMA$, cache);

    TestRecord testRecord = FastSpecificDeserializerGeneratorTest.emptyTestRecord();
    testRecord.testEnum = TestEnum.A;

    // when
    fastSpecificDatumWriter.write(testRecord,
        AvroCompatibilityHelper.newBufferedBinaryEncoder(new ByteArrayOutputStream()));

    // then
    FastSerializer<TestRecord> fastSpecificSerializer =
        (FastSerializer<TestRecord>) cache.getFastSpecificSerializer(TestRecord.SCHEMA$);

    fastSpecificSerializer = (FastSerializer<TestRecord>) cache.getFastSpecificSerializer(TestRecord.SCHEMA$);

    Assert.assertNotNull(fastSpecificSerializer);
    Assert.assertNotEquals(2, fastSpecificSerializer.getClass().getDeclaredMethods().length);
  }

  @Test(groups = {"serializationTest"})
  @SuppressWarnings("unchecked")
  public void shouldCreateGenericDatumReader() throws IOException, InterruptedException {
    Schema recordSchema = createRecord("TestSchema", createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
    FastGenericDatumWriter<GenericRecord> fastGenericDatumReader = new FastGenericDatumWriter<>(recordSchema, cache);

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("test", "test");

    // when
    fastGenericDatumReader.write(record, AvroCompatibilityHelper.newBufferedBinaryEncoder(new ByteArrayOutputStream()));

    // then
    FastSerializer<GenericRecord> fastGenericSerializer =
        (FastSerializer<GenericRecord>) cache.getFastGenericSerializer(recordSchema);

    fastGenericSerializer = (FastSerializer<GenericRecord>) cache.getFastGenericSerializer(recordSchema);

    Assert.assertNotNull(fastGenericSerializer);
    Assert.assertNotEquals(2, fastGenericSerializer.getClass().getDeclaredMethods().length);
  }
}
