package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.SubRecord;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
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

    setField(record, "testFixed", testFixed1);
    setField(record, "testFixedArray", Collections.EMPTY_LIST);
    setField(record, "testFixedUnionArray", Arrays.asList(testFixed1));

    setField(record, "testEnum", TestEnum.A);
    setField(record, "testEnumArray", Collections.EMPTY_LIST);
    setField(record, "testEnumUnionArray", Arrays.asList(TestEnum.A));
    setField(record, "subRecord", new SubRecord());

    setField(record, "recordsArray", Collections.emptyList());
    setField(record, "recordsArrayMap", Collections.emptyList());
    setField(record, "recordsMap", Collections.emptyMap());
    setField(record, "recordsMapArray", Collections.emptyMap());

    setField(record, "testInt", 1);
    setField(record, "testLong", 1l);
    setField(record, "testDouble", 1.0);
    setField(record, "testFloat", 1.0f);
    setField(record, "testBoolean", true);
    setField(record, "testString", "aaa");
    setField(record, "testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));

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
    setField(record, "testInt", 1);
    setField(record, "testIntUnion", 1);
    setField(record, "testString", "aaa");
    setField(record, "testStringUnion", "aaa");
    setField(record, "testLong", 1l);
    setField(record, "testLongUnion", 1l);
    setField(record, "testDouble", 1.0);
    setField(record, "testDoubleUnion", 1.0);
    setField(record, "testFloat", 1.0f);
    setField(record, "testFloatUnion", 1.0f);
    setField(record, "testBoolean", true);
    setField(record, "testBooleanUnion", true);
    setField(record, "testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    setField(record, "testBytesUnion", ByteBuffer.wrap(new byte[]{0x01, 0x02}));

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(1, getField(record, "testInt"));
    Assert.assertEquals(1, ((Integer) getField(record, "testIntUnion")).intValue());
    Assert.assertEquals("aaa", getField(record, "testString").toString());
    Assert.assertEquals("aaa", getField(record, "testStringUnion").toString());
    Assert.assertEquals(1l, getField(record, "testLong"));
    Assert.assertEquals(1l, ((Long) getField(record, "testLongUnion")).longValue());
    Assert.assertEquals(1.0, getField(record, "testDouble"));
    Assert.assertEquals(1.0, getField(record, "testDoubleUnion"));
    Assert.assertEquals(1.0f, getField(record, "testFloat"));
    Assert.assertEquals(1.0f, getField(record, "testFloatUnion"));
    Assert.assertEquals(true, getField(record, "testBoolean"));
    Assert.assertEquals(true, ((Boolean) getField(record, "testBooleanUnion")).booleanValue());
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), getField(record, "testBytes"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), getField(record, "testBytesUnion"));
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

    setField(record, "testFixed", testFixed1);
    setField(record, "testFixedUnion", testFixed2);
    setField(record, "testFixedArray", Arrays.asList(testFixed3));
    setField(record, "testFixedUnionArray", Arrays.asList(testFixed4));

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(new byte[]{0x01}, ((TestFixed) getField(record, "testFixed")).bytes());
    Assert.assertEquals(new byte[]{0x02}, ((TestFixed) getField(record, "testFixedUnion")).bytes());
    Assert.assertEquals(new byte[]{0x03}, ((List<TestFixed>) getField(record, "testFixedArray")).get(0).bytes());
    Assert.assertEquals(new byte[]{0x04}, ((List<TestFixed>) getField(record, "testFixedUnionArray")).get(0).bytes());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteEnum() {
    // given
    TestRecord record = emptyTestRecord();

    setField(record, "testEnum", TestEnum.A);
    setField(record, "testEnumUnion", TestEnum.A);
    setField(record, "testEnumArray", Arrays.asList(TestEnum.A));
    setField(record, "testEnumUnionArray", Arrays.asList(TestEnum.A));

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(TestEnum.A, getField(record, "testEnum"));
    Assert.assertEquals(TestEnum.A, getField(record, "testEnumUnion"));
    Assert.assertEquals(TestEnum.A, ((List<TestEnum>) getField(record, "testEnumArray")).get(0));
    Assert.assertEquals(TestEnum.A, ((List<TestEnum>) getField(record, "testEnumUnionArray")).get(0));
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordField() {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    setField(subRecord, "subField", "abc");

    setField(record, "subRecordUnion", subRecord);
    setField(record, "subRecord", subRecord);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", getField((SubRecord) getField(record, "subRecordUnion"), "subField").toString());
    Assert.assertEquals("abc", getField((SubRecord) getField(record, "subRecord"), "subField").toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordCollectionsField() {

    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    setField(subRecord, "subField", "abc");

    List<SubRecord> recordsArray = new ArrayList<>();
    recordsArray.add(subRecord);
    setField(record, "recordsArray", recordsArray);
    setField(record, "recordsArrayUnion", recordsArray);
    Map<CharSequence, SubRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecord);
    setField(record, "recordsMap", recordsMap);
    setField(record, "recordsMapUnion", recordsMap);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", getField(((List<SubRecord>) getField(record, "recordsArray")).get(0), "subField").toString());
    Assert.assertEquals("abc", getField(((List<SubRecord>) getField(record, "recordsArrayUnion")).get(0), "subField").toString());
    Assert.assertEquals("abc", getField(((Map<CharSequence, SubRecord>) getField(record, "recordsMap")).get(new Utf8("1")), "subField").toString());
    Assert.assertEquals("abc", getField(((Map<CharSequence, SubRecord>) getField(record, "recordsMapUnion")).get(new Utf8("1")), "subField").toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteSubRecordComplexCollectionsField() {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    setField(subRecord, "subField", "abc");

    List<Map<CharSequence, SubRecord>> recordsArrayMap = new ArrayList<>();
    Map<CharSequence, SubRecord> recordMap = new HashMap<>();

    recordMap.put("1", subRecord);
    recordsArrayMap.add(recordMap);

    setField(record, "recordsArrayMap", recordsArrayMap);
    setField(record, "recordsArrayMapUnion", recordsArrayMap);

    Map<CharSequence, List<SubRecord>> recordsMapArray = new HashMap<>();
    List<SubRecord> recordList = new ArrayList<>();
    recordList.add(subRecord);
    recordsMapArray.put("1", recordList);

    setField(record, "recordsMapArray", recordsMapArray);
    setField(record, "recordsMapArrayUnion", recordsMapArray);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", getField(((List<Map<CharSequence, SubRecord>>) getField(record, "recordsArrayMap")).get(0).get(new Utf8("1")), "subField").toString());
    Assert.assertEquals("abc", getField(((Map<CharSequence, List<SubRecord>>) getField(record, "recordsMapArray")).get(new Utf8("1")).get(0), "subField").toString());
    Assert.assertEquals("abc", getField(((List<Map<CharSequence, SubRecord>>) getField(record, "recordsArrayMapUnion")).get(0).get(new Utf8("1")), "subField").toString());
    Assert.assertEquals("abc", getField(((Map<CharSequence, List<SubRecord>>) getField(record, "recordsMapArrayUnion")).get(new Utf8("1")).get(0), "subField").toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteMultipleChoiceUnion() {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    setField(subRecord, "subField", "abc");
    setField(record, "union", subRecord);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", getField(((SubRecord) getField(record, "union")), "subField").toString());

    // given
    setField(record, "union", "abc");

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals("abc", getField(record, "union").toString());

    // given
    setField(record, "union", 1);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, dataAsDecoder(record));

    // then
    Assert.assertEquals(1, getField(record, "union"));
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteArrayOfRecords() {
    // given
    Schema arrayRecordSchema = Schema.createArray(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    setField(testRecord, "testString", "abc");

    List<TestRecord> recordsArray = new ArrayList<>();
    recordsArray.add(testRecord);
    recordsArray.add(testRecord);

    // when
    List<TestRecord> array = decodeRecordFast(arrayRecordSchema, dataAsDecoder(recordsArray, arrayRecordSchema));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals("abc", getField(array.get(0), "testString").toString());
    Assert.assertEquals("abc", getField(array.get(1), "testString").toString());

    // given
    testRecord = emptyTestRecord();
    setField(testRecord, "testString", "abc");

    arrayRecordSchema = Schema.createArray(createUnionSchema(TestRecord.SCHEMA$));

    recordsArray = new ArrayList<>();
    recordsArray.add(testRecord);
    recordsArray.add(testRecord);

    // when
    array = decodeRecordFast(arrayRecordSchema, dataAsDecoder(recordsArray, arrayRecordSchema));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals("abc", getField(array.get(0), "testString").toString());
    Assert.assertEquals("abc", getField(array.get(1), "testString").toString());
  }

  @Test(groups = {"serializationTest"})
  public void shouldWriteMapOfRecords() {
    // given
    Schema mapRecordSchema = Schema.createMap(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    setField(testRecord, "testString", "abc");

    Map<String, TestRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", testRecord);
    recordsMap.put("2", testRecord);

    // when
    Map<String, TestRecord> map = decodeRecordFast(mapRecordSchema, dataAsDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("abc", getField(map.get(new Utf8("1")), "testString").toString());
    Assert.assertEquals("abc", getField(map.get(new Utf8("2")), "testString").toString());

    // given
    mapRecordSchema = Schema.createMap(createUnionSchema(TestRecord.SCHEMA$));

    testRecord = emptyTestRecord();
    setField(testRecord, "testString", "abc");

    recordsMap = new HashMap<>();
    recordsMap.put("1", testRecord);
    recordsMap.put("2", testRecord);

    // when
    map = decodeRecordFast(mapRecordSchema, dataAsDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("abc", getField(map.get(new Utf8("1")), "testString").toString());
    Assert.assertEquals("abc", getField(map.get(new Utf8("2")), "testString").toString());
  }

  public <T extends GenericContainer> Decoder dataAsDecoder(T data) {
    return dataAsDecoder(data, data.getSchema());
  }

  public <T> Decoder dataAsDecoder(T data, Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null);

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
