package com.linkedin.avro.fastserde;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.fastserde.generated.avro.SubRecord;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastSpecificSerializerGeneratorTest {

  private File tempDir;
  private ClassLoader classLoader;

  public static TestRecord emptyTestRecord() {
    TestRecord record = new TestRecord();

    TestFixed testFixed1 = new TestFixed();
    testFixed1.bytes(new byte[]{0x01});

    record.testFixed = testFixed1;
    record.testFixedArray = Collections.EMPTY_LIST;
    record.testFixedUnionArray = Arrays.asList(testFixed1);

    record.testEnum = TestEnum.A;
    record.testEnumArray = Collections.EMPTY_LIST;
    record.testEnumUnionArray = Arrays.asList(TestEnum.A);
    record.subRecord = new SubRecord();

    record.recordsArray = Collections.emptyList();
    record.recordsArrayMap = Collections.emptyList();
    record.recordsMap = Collections.emptyMap();
    record.recordsMapArray = Collections.emptyMap();

    record.testInt = 1;
    record.testLong = 1l;
    record.testDouble = 1.0;
    record.testFloat = 1.0f;
    record.testBoolean = true;
    record.testString = "aaa";
    record.testBytes = ByteBuffer.wrap(new byte[]{0x01, 0x02});

    return record;
  }

  @BeforeTest(groups = {"serializationTest"})
  public void prepare() throws Exception {
    Path tempPath = Files.createTempDirectory("generated");
    tempDir = tempPath.toFile();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastSpecificSerializerGeneratorTest.class.getClassLoader());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWritePrimitives() {
    TestRecord record = emptyTestRecord();
    record.testInt = 1;
    record.testIntUnion = 1;
    record.testString = "aaa";
    record.testStringUnion = "aaa";
    record.testLong = 1l;
    record.testLongUnion = 1l;
    record.testDouble = 1.0;
    record.testDoubleUnion = 1.0;
    record.testFloat = 1.0f;
    record.testFloatUnion = 1.0f;
    record.testBoolean = true;
    record.testBooleanUnion = true;
    record.testBytes = ByteBuffer.wrap(new byte[]{0x01, 0x02});
    record.testBytesUnion = ByteBuffer.wrap(new byte[]{0x01, 0x02});

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(1, record.testInt);
    Assert.assertEquals(1, record.testIntUnion.intValue());
    Assert.assertEquals("aaa", record.testString.toString());
    Assert.assertEquals("aaa", record.testStringUnion.toString());
    Assert.assertEquals(1l, record.testLong);
    Assert.assertEquals(1l, record.testLongUnion.longValue());
    Assert.assertEquals(1.0, record.testDouble);
    Assert.assertEquals(1.0, record.testDoubleUnion);
    Assert.assertEquals(1.0f, record.testFloat);
    Assert.assertEquals(1.0f, record.testFloatUnion);
    Assert.assertEquals(true, record.testBoolean);
    Assert.assertEquals(true, record.testBooleanUnion.booleanValue());
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.testBytes);
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.testBytesUnion);
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteFixed() {
    // given
    TestRecord record = emptyTestRecord();

    TestFixed testFixed1 = new TestFixed();
    testFixed1.bytes(new byte[]{0x01});
    TestFixed testFixed2 = new TestFixed();
    testFixed2.bytes(new byte[]{0x02});
    TestFixed testFixed3 = new TestFixed();
    testFixed3.bytes(new byte[]{0x03});
    TestFixed testFixed4 = new TestFixed();
    testFixed4.bytes(new byte[]{0x04});

    record.testFixed = testFixed1;
    record.testFixedUnion = testFixed2;
    record.testFixedArray = Arrays.asList(testFixed3);
    record.testFixedUnionArray = Arrays.asList(testFixed4);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(new byte[]{0x01}, record.testFixed.bytes());
    Assert.assertEquals(new byte[]{0x02}, record.testFixedUnion.bytes());
    Assert.assertEquals(new byte[]{0x03}, record.testFixedArray.get(0).bytes());
    Assert.assertEquals(new byte[]{0x04}, record.testFixedUnionArray.get(0).bytes());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteEnum() {
    // given
    TestRecord record = emptyTestRecord();

    record.testEnum = TestEnum.A;
    record.testEnumUnion = TestEnum.A;
    record.testEnumArray = Arrays.asList(TestEnum.A);
    record.testEnumUnionArray = Arrays.asList(TestEnum.A);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(TestEnum.A, record.testEnum);
    Assert.assertEquals(TestEnum.A, record.testEnumUnion);
    Assert.assertEquals(TestEnum.A, record.testEnumArray.get(0));
    Assert.assertEquals(TestEnum.A, record.testEnumUnionArray.get(0));
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordField() {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    subRecord.subField = "abc";

    record.subRecordUnion = subRecord;
    record.subRecord = subRecord;

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", record.subRecordUnion.subField.toString());
    Assert.assertEquals("abc", record.subRecord.subField.toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordCollectionsField() {

    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    subRecord.subField = "abc";

    List<SubRecord> recordsArray = new ArrayList<>();
    recordsArray.add(subRecord);
    record.recordsArray = recordsArray;
    record.recordsArrayUnion = recordsArray;
    Map<CharSequence, SubRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecord);
    record.recordsMap = recordsMap;
    record.recordsMapUnion = recordsMap;

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", record.recordsArray.get(0).subField.toString());
    Assert.assertEquals("abc", record.recordsArrayUnion.get(0).subField.toString());
    Assert.assertEquals("abc", record.recordsMap.get(new Utf8("1")).subField.toString());
    Assert.assertEquals("abc", record.recordsMapUnion.get(new Utf8("1")).subField.toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordComplexCollectionsField() {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    subRecord.subField = "abc";

    List<Map<CharSequence, SubRecord>> recordsArrayMap = new ArrayList<>();
    Map<CharSequence, SubRecord> recordMap = new HashMap<>();

    recordMap.put("1", subRecord);
    recordsArrayMap.add(recordMap);

    record.recordsArrayMap = recordsArrayMap;
    record.recordsArrayMapUnion = recordsArrayMap;

    Map<CharSequence, List<SubRecord>> recordsMapArray = new HashMap<>();
    List<SubRecord> recordList = new ArrayList<>();
    recordList.add(subRecord);
    recordsMapArray.put("1", recordList);

    record.recordsMapArray = recordsMapArray;
    record.recordsMapArrayUnion = recordsMapArray;

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", record.recordsArrayMap.get(0).get(new Utf8("1")).subField.toString());
    Assert.assertEquals("abc", record.recordsMapArray.get(new Utf8("1")).get(0).subField.toString());
    Assert.assertEquals("abc", record.recordsArrayMapUnion.get(0).get(new Utf8("1")).subField.toString());
    Assert.assertEquals("abc", record.recordsMapArrayUnion.get(new Utf8("1")).get(0).subField.toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteMultipleChoiceUnion() {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    subRecord.subField = "abc";
    record.union = subRecord;

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", ((SubRecord) record.union).subField.toString());

    // given
    record.union = "abc";

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", record.union.toString());

    // given
    record.union = 1;

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(1, record.union);
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfRecords() {
    // given
    Schema arrayRecordSchema = Schema.createArray(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    testRecord.testString = "abc";

    List<TestRecord> recordsArray = new ArrayList<>();
    recordsArray.add(testRecord);
    recordsArray.add(testRecord);

    // when
    List<TestRecord> array = decodeRecordFast(arrayRecordSchema, dataAsDecoder(recordsArray, arrayRecordSchema));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals("abc", array.get(0).testString.toString());
    Assert.assertEquals("abc", array.get(1).testString.toString());

    // given
    testRecord = emptyTestRecord();
    testRecord.testString = "abc";

    arrayRecordSchema = Schema.createArray(createUnionSchema(TestRecord.SCHEMA$));

    recordsArray = new ArrayList<>();
    recordsArray.add(testRecord);
    recordsArray.add(testRecord);

    // when
    array = decodeRecordFast(arrayRecordSchema, dataAsDecoder(recordsArray, arrayRecordSchema));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals("abc", array.get(0).testString.toString());
    Assert.assertEquals("abc", array.get(1).testString.toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteMapOfRecords() {
    // given
    Schema mapRecordSchema = Schema.createMap(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    testRecord.testString = "abc";

    Map<String, TestRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", testRecord);
    recordsMap.put("2", testRecord);

    // when
    Map<String, TestRecord> map = decodeRecordFast(mapRecordSchema, dataAsDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("abc", map.get(new Utf8("1")).testString.toString());
    Assert.assertEquals("abc", map.get(new Utf8("2")).testString.toString());

    // given
    mapRecordSchema = Schema.createMap(createUnionSchema(TestRecord.SCHEMA$));

    testRecord = emptyTestRecord();
    testRecord.testString = "abc";

    recordsMap = new HashMap<>();
    recordsMap.put("1", testRecord);
    recordsMap.put("2", testRecord);

    // when
    map = decodeRecordFast(mapRecordSchema, dataAsDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("abc", map.get(new Utf8("1")).testString.toString());
    Assert.assertEquals("abc", map.get(new Utf8("2")).testString.toString());
  }

  public <T extends GenericContainer> Decoder dataAsDecoder(T data) {
    return dataAsDecoder(data, data.getSchema());
  }

  public <T> Decoder dataAsDecoder(T data, Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBufferedBinaryEncoder(baos);

    try {
      FastSpecificSerializerGenerator<T> fastSpecificSerializerGenerator =
          new FastSpecificSerializerGenerator<>(schema, tempDir, classLoader, null);
      FastSerializer<T> fastSerializer = fastSpecificSerializerGenerator.generateSerializer();
      fastSerializer.serialize(data, binaryEncoder);
      binaryEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray(), null);
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeRecordFast(Schema writerSchema, Decoder decoder) {
    SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema);
    try {
      return datumReader.read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
