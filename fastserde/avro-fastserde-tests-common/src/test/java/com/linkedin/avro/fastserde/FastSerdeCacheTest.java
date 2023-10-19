package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.SimpleRecord;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastSerdeCacheTest {

  @Test(groups = "deserializationTest")
  public void testIsSupportedForFastDeserializer() {
    Set<Schema.Type> supportedSchemaTypes = new HashSet<>();
    supportedSchemaTypes.add(Schema.Type.RECORD);
    supportedSchemaTypes.add(Schema.Type.MAP);
    supportedSchemaTypes.add(Schema.Type.ARRAY);

    Map<Schema.Type, Schema> schemaTypes = new HashMap<>();
    /*
     * Those types could be created by {@link Schema#create(org.apache.avro.Schema.Type)} function.
     */
    schemaTypes.put(Schema.Type.RECORD, Schema.parse("{\"type\": \"record\", \"name\": \"test\", \"fields\":[]}"));
    schemaTypes.put(Schema.Type.MAP, Schema.parse("{\"type\": \"map\", \"values\": \"string\"}"));
    schemaTypes.put(Schema.Type.ARRAY, Schema.parse("{\"type\": \"array\", \"items\": \"string\"}"));
    schemaTypes.put(Schema.Type.ENUM, Schema.parse("{\"type\": \"enum\", \"name\": \"test_enum\", \"symbols\":[]}"));
    schemaTypes.put(Schema.Type.UNION, Schema.parse("[\"null\", \"string\"]"));
    schemaTypes.put(Schema.Type.FIXED, Schema.parse("{\"type\": \"fixed\", \"size\": 16, \"name\": \"test_fixed\"}"));

    for (Schema.Type type : Schema.Type.values()) {
      Schema schema = schemaTypes.containsKey(type) ? schemaTypes.get(type) : Schema.create(type);
      if (supportedSchemaTypes.contains(type)) {
        Assert.assertTrue(FastSerdeCache.isSupportedForFastDeserializer(type));
        FastDeserializerGeneratorBase.getClassName(schema, schema, "");
      } else {
        Assert.assertFalse(FastSerdeCache.isSupportedForFastDeserializer(type));
      }
    }
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastGenericDeserializerSurviveFromWrongClasspath() {
    String wrongClasspath = ".";
    FastSerdeCache cache = new FastSerdeCache(wrongClasspath);
    Schema testRecord = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    cache.buildFastGenericDeserializer(testRecord, testRecord);
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastGenericDeserializerWithCorrectClasspath() {
    FastSerdeCache cache = FastSerdeCache.getDefaultInstance();
    Schema testRecord = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    cache.buildFastGenericDeserializer(testRecord, testRecord);
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastSpecificDeserializerSurviveFromWrongClasspath() {
    String wrongClasspath = ".";
    FastSerdeCache cache = new FastSerdeCache(wrongClasspath);
    cache.buildFastSpecificDeserializer(TestRecord.SCHEMA$, TestRecord.SCHEMA$);
  }

  @Test(groups = "deserializationTest")
  public void testBuildFastSpecificDeserializerWithCorrectClasspath() {
    FastSerdeCache cache = FastSerdeCache.getDefaultInstance();
    cache.buildFastSpecificDeserializer(TestRecord.SCHEMA$, TestRecord.SCHEMA$);
  }

  @Test(groups = "serializationTest", timeOut = 5_000L,
          expectedExceptions = UnsupportedOperationException.class,
          expectedExceptionsMessageRegExp = "Fast specific serializer could not be generated.")
  public void testSpecificSerializationFailsFast() throws Exception {
    serializationShouldFailFast(FastSpecificDatumWriter::new);
  }

  @Test(groups = "serializationTest", timeOut = 5_000L,
          expectedExceptions = UnsupportedOperationException.class,
          expectedExceptionsMessageRegExp = "Fast generic serializer could not be generated.")
  public void testGenericSerializationFailsFast() throws Exception {
    serializationShouldFailFast(FastGenericDatumWriter::new);
  }

  @Test(groups = "deserializationTest", timeOut = 5_000L,
          expectedExceptions = UnsupportedOperationException.class,
          expectedExceptionsMessageRegExp = "Fast specific deserializer could not be generated.")
  public void testSpecificDeserializationFailsFast() throws Exception {
    deserializationShouldFailFast(FastSpecificDatumReader::new);
  }

  @Test(groups = "deserializationTest", timeOut = 5_000L,
          expectedExceptions = UnsupportedOperationException.class,
          expectedExceptionsMessageRegExp = "Fast generic deserializer could not be generated.")
  public void testGenericDeserializationFailsFast() throws Exception {
    deserializationShouldFailFast(FastGenericDatumReader::new);
  }

  private void serializationShouldFailFast(
          BiFunction<Schema, FastSerdeCache, DatumWriter<SimpleRecord>> datumWriterFactory) throws Exception {
    // given:
    SimpleRecord data = new SimpleRecord();
    data.put(0, "Veni, vidi, vici.");
    FastSerdeCache cache = createCacheWithoutClassLoader();
    DatumWriter<SimpleRecord> writer = datumWriterFactory.apply(data.getSchema(), cache);

    int i = 0;
    while (++i <= 100) {
      BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(new ByteArrayOutputStream());
      // should throw exception (except 1st iteration when fallback writer is always used)
      writer.write(data, encoder);
      Thread.sleep(50L);
    }
  }

  private void deserializationShouldFailFast(
          BiFunction<Schema, FastSerdeCache, DatumReader<SimpleRecord>> datumReaderFactory) throws Exception {
    // given
    SimpleRecord data = new SimpleRecord();
    data.put(0, "Omnes una manet nox.");
    FastSerdeCache cache = createCacheWithoutClassLoader();

    SpecificDatumWriter<SimpleRecord> specificDatumWriter = new SpecificDatumWriter<>(data.getSchema());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = AvroCompatibilityHelper.newBinaryEncoder(baos);
    specificDatumWriter.write(data, encoder);
    encoder.flush();

    DatumReader<SimpleRecord> datumReader = datumReaderFactory.apply(data.getSchema(), cache);

    int i = 0;
    while (++i <= 100) {
      BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(baos.toByteArray());
      // should throw exception (except 1st iteration when fallback reader is always used)
      datumReader.read(null, decoder);
      Thread.sleep(50L);
    }
  }

  private FastSerdeCache createCacheWithoutClassLoader() throws IllegalAccessException, NoSuchFieldException {
    FastSerdeCache cache = new FastSerdeCache(null, null, true);
    Field classLoaderField = cache.getClass().getDeclaredField("classLoader");
    classLoaderField.setAccessible(true);
    classLoaderField.set(cache, null); // so that an exception is thrown while compiling generated class
    return cache;
  }
}
