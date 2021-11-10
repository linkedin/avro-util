package com.linkedin.avro.fastserde;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FastDatumReaderWriterUtilTest {

  @Test (groups = "serializationTest")
  public void testIsSupportedForFastGenericDatumWriter() {
    Schema testSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastGenericDatumWriter fastWriter = FastDatumReaderWriterUtil.getFastGenericDatumWriter(testSchema);
    Assert.assertNotNull(fastWriter);
    FastGenericDatumWriter newFastWriter = FastDatumReaderWriterUtil.getFastGenericDatumWriter(testSchema);
    Assert.assertSame(fastWriter, newFastWriter);
  }

  @Test (groups = "deserializationTest")
  public void testIsSupportedForFastGenericDatumReader() {
    Schema testWriterSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    Schema testReaderSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastGenericDatumReader fastReader = FastDatumReaderWriterUtil.getFastGenericDatumReader(testWriterSchema, testReaderSchema);
    Assert.assertNotNull(fastReader);
    FastGenericDatumReader newFastReader = FastDatumReaderWriterUtil.getFastGenericDatumReader(testWriterSchema, testReaderSchema);
    Assert.assertSame(fastReader, newFastReader);
  }

  @Test (groups = "deserializationTest")
  public void testIsSupportedForFastGenericDatumReaderWithSameReaderWriterSchema() {
    Schema testSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastGenericDatumReader fastReader = FastDatumReaderWriterUtil.getFastGenericDatumReader(testSchema);
    Assert.assertNotNull(fastReader);
    FastGenericDatumReader newFastReader = FastDatumReaderWriterUtil.getFastGenericDatumReader(testSchema);
    Assert.assertSame(fastReader, newFastReader);
  }

  @Test (groups = "serializationTest")
  public void testIsSupportedForFastSpecificDatumWriter() {
    Schema testSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastSpecificDatumWriter fastWriter = FastDatumReaderWriterUtil.getFastSpecificDatumWriter(testSchema);
    Assert.assertNotNull(fastWriter);
    FastSpecificDatumWriter newFastWriter = FastDatumReaderWriterUtil.getFastSpecificDatumWriter(testSchema);
    Assert.assertSame(fastWriter, newFastWriter);
  }

  @Test (groups = "deserializationTest")
  public void testIsSupportedForFastSpecificDatumReader() {
    Schema testWriterSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    Schema testReaderSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastSpecificDatumReader fastReader = FastDatumReaderWriterUtil.getFastSpecificDatumReader(testWriterSchema, testReaderSchema);
    Assert.assertNotNull(fastReader);
    FastSpecificDatumReader newFastReader = FastDatumReaderWriterUtil.getFastSpecificDatumReader(testWriterSchema, testReaderSchema);
    Assert.assertSame(fastReader, newFastReader);
  }

  @Test (groups = "deserializationTest")
  public void testIsSupportedForFastSpecificDatumReaderWithSameReaderWriterSchema() {
    Schema testSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastSpecificDatumReader fastReader = FastDatumReaderWriterUtil.getFastSpecificDatumReader(testSchema);
    Assert.assertNotNull(fastReader);
    FastSpecificDatumReader newFastReader = FastDatumReaderWriterUtil.getFastSpecificDatumReader(testSchema);
    Assert.assertSame(fastReader, newFastReader);
  }

  @Test (groups = "deserializationTest")
  public void testIsSupportedForFastGenericDatumReaderWarmUp() {
    Schema testSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastDatumReaderWriterUtil.warmUpFastGenericDatumReader(testSchema, testSchema);
    FastGenericDatumReader fastReader = FastDatumReaderWriterUtil.getFastGenericDatumReader(testSchema, testSchema);
    Assert.assertTrue(fastReader.isFastDeserializerUsed());
  }

  @Test (groups = "deserializationTest")
  public void testIsSupportedForFastSpecificDatumReaderWarmUp() {
    Schema testSchema = Schema.parse("{\"type\": \"record\", \"name\": \"test_record\", \"fields\":[]}");
    FastDatumReaderWriterUtil.warmUpFastSpecificDatumReader(testSchema, testSchema);
    FastSpecificDatumReader fastReader = FastDatumReaderWriterUtil.getFastSpecificDatumReader(testSchema);
    Assert.assertTrue(fastReader.isFastDeserializerUsed());
  }
}
