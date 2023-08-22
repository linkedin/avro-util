package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
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
    setField(testRecord, "testEnum", TestEnum.A);

    // when
    fastSpecificDatumWriter.write(testRecord,
        AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream(), true, null));

    // then
    FastSerializer<TestRecord> fastSpecificSerializer =
        (FastSerializer<TestRecord>) cache.getFastSpecificSerializer(TestRecord.SCHEMA$);

    fastSpecificSerializer = (FastSerializer<TestRecord>) cache.getFastSpecificSerializer(TestRecord.SCHEMA$);

    Assert.assertNotNull(fastSpecificSerializer);
    Assert.assertNotEquals(2, fastSpecificSerializer.getClass().getDeclaredMethods().length);
  }

  @Test(groups = {"serializationTest"})
  @SuppressWarnings("unchecked")
  public void shouldCreateGenericDatumWriter() throws IOException {
    Schema recordSchema = createRecord("TestSchema", createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
    FastGenericDatumWriter<GenericRecord, GenericData> fastGenericDatumWriter = new FastGenericDatumWriter<>(recordSchema, cache);

    Assert.assertFalse(fastGenericDatumWriter.isFastSerializerUsed(), "FastGenericDatumWriter"
        + " shouldn't use the fast serializer when firstly created");

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("test", "test");

    // when
    fastGenericDatumWriter.write(record, AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream(), true, null));
    Assert.assertFalse(fastGenericDatumWriter.isFastSerializerUsed(), "FastGenericDatumWriter shouldn't"
        + " use the fast serializer during fast class generation");

    // then
    FastSerializer<GenericRecord> fastGenericSerializer =
        (FastSerializer<GenericRecord>) cache.getFastGenericSerializer(recordSchema);

    fastGenericSerializer = (FastSerializer<GenericRecord>) cache.getFastGenericSerializer(recordSchema);

    Assert.assertNotNull(fastGenericSerializer);
    Assert.assertNotEquals(2, fastGenericSerializer.getClass().getDeclaredMethods().length);

    // Block fast class generation
    cache.buildFastGenericSerializer(recordSchema, null);
    fastGenericDatumWriter.write(record, AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream(), true, null));
    Assert.assertTrue(fastGenericDatumWriter.isFastSerializerUsed(), "FastGenericDatumWriter should be using"
        + " Fast Serializer when the fast deserializer generation is done.");
  }
}
