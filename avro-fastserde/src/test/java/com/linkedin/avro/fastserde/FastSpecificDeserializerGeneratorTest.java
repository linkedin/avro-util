package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord;
import com.linkedin.avro.fastserde.generated.avro.SubRecord;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastSpecificDeserializerGeneratorTest {

  private File tempDir;
  private ClassLoader classLoader;

  @DataProvider(name = "SlowFastDeserializer")
  public static Object[][] deserializers() {
    return new Object[][]{{true}, {false}};
  }

  public static TestRecord emptyTestRecord() {
    TestRecord record = new TestRecord();

    TestFixed testFixed1 = new TestFixed();
    testFixed1.bytes(new byte[]{0x01});
    record.testFixed = testFixed1;
    record.testFixedArray = Collections.emptyList();
    TestFixed testFixed2 = new TestFixed();
    testFixed2.bytes(new byte[]{0x01});
    record.testFixedUnionArray = Arrays.asList(testFixed2);

    record.testEnum = TestEnum.A;
    record.testEnumArray = Collections.emptyList();
    record.testEnumUnionArray = Arrays.asList(TestEnum.A);
    record.booleanArray = Collections.emptyList();
    record.doubleArray = Collections.emptyList();
    record.floatArray = Collections.emptyList();
    record.intArray = Collections.emptyList();
    record.longArray = Collections.emptyList();
    record.stringArray = Collections.emptyList();
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

  @BeforeTest(groups = {"deserializationTest"})
  public void prepare() throws Exception {
    tempDir = getCodeGenDirectory();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastSpecificDeserializerGeneratorTest.class.getClassLoader());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadPrimitives(Boolean whetherUseFastDeserializer) {
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
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(1, record.testInt);
    Assert.assertEquals(new Integer(1), record.testIntUnion);
    Assert.assertEquals(new Utf8("aaa"), record.testString);
    Assert.assertEquals(new Utf8("aaa"), record.testStringUnion);
    Assert.assertEquals(1l, record.testLong);
    Assert.assertEquals(new Long(1), record.testLongUnion);
    Assert.assertEquals(1.0, record.testDouble, 0);
    Assert.assertEquals(new Double(1.0), record.testDoubleUnion);
    Assert.assertEquals(1.0f, record.testFloat, 0);
    Assert.assertEquals(new Float(1.0f), record.testFloatUnion);
    Assert.assertEquals(true, record.testBoolean);
    Assert.assertEquals(new Boolean(true), record.testBooleanUnion);
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.testBytes);
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.testBytesUnion);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadFixed(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();

    TestFixed testFixed1 = new TestFixed();
    testFixed1.bytes(new byte[]{0x01});
    record.testFixed = testFixed1;
    TestFixed testFixed2 = new TestFixed();
    testFixed2.bytes(new byte[]{0x02});
    record.testFixedUnion = testFixed2;
    TestFixed testFixed3 = new TestFixed();
    testFixed3.bytes(new byte[]{0x03});
    TestFixed testFixed4 = new TestFixed();
    testFixed4.bytes(new byte[]{0x04});
    record.testFixedArray = Arrays.asList(testFixed3);
    record.testFixedUnionArray = Arrays.asList(testFixed4);

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new byte[]{0x01}, record.testFixed.bytes());
    Assert.assertEquals(new byte[]{0x02}, record.testFixedUnion.bytes());
    Assert.assertEquals(new byte[]{0x03}, record.testFixedArray.get(0).bytes());
    Assert.assertEquals(new byte[]{0x04}, record.testFixedUnionArray.get(0).bytes());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadEnum(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();

    record.testEnum = TestEnum.A;
    record.testEnumUnion = TestEnum.A;
    record.testEnumArray = Arrays.asList(TestEnum.A);
    record.testEnumUnionArray = Arrays.asList(TestEnum.A);

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(TestEnum.A, record.testEnum);
    Assert.assertEquals(TestEnum.A, record.testEnumUnion);
    Assert.assertEquals(TestEnum.A, record.testEnumArray.get(0));
    Assert.assertEquals(TestEnum.A, record.testEnumUnionArray.get(0));
  }

  public GenericData.Fixed newFixed(Schema fixedSchema, byte[] bytes) {
    GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema);
    fixed.bytes(bytes);
    return fixed;
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadPermutatedEnum(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/fastserdetestold.avsc"));
    GenericData.Fixed testFixed = newFixed(oldRecordSchema.getField("testFixed").schema(), new byte[]{0x01});
    GenericData.Record subRecord =
        new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema().getTypes().get(1));
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    oldRecord.put("testInt", 1);
    oldRecord.put("testLong", 1l);
    oldRecord.put("testDouble", 1.0);
    oldRecord.put("testFloat", 1.0f);
    oldRecord.put("testBoolean", true);
    oldRecord.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    oldRecord.put("testString", "aaa");
    oldRecord.put("testFixed", testFixed);
    oldRecord.put("testFixedUnion", testFixed);
    oldRecord.put("testFixedArray", Arrays.asList(testFixed));
    oldRecord.put("testFixedUnionArray", Arrays.asList(testFixed));
    oldRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "A")); //new GenericData.EnumSymbol("A"));
    oldRecord.put("testEnumUnion",
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "B")); //new GenericData.EnumSymbol("B"));
    oldRecord.put("testEnumArray", Arrays.asList(
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "C"))); //new GenericData.EnumSymbol("C")));
    oldRecord.put("testEnumUnionArray", Arrays.asList(
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "D"))); //new GenericData.EnumSymbol("D")));

    oldRecord.put("subRecordUnion", subRecord);
    oldRecord.put("subRecord", subRecord);

    oldRecord.put("recordsArray", Collections.emptyList());
    oldRecord.put("recordsArrayMap", Collections.emptyList());
    oldRecord.put("recordsMap", Collections.emptyMap());
    oldRecord.put("recordsMapArray", Collections.emptyMap());

    // when
    TestRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    }

    // then
    Assert.assertEquals(TestEnum.A, record.testEnum);
    Assert.assertEquals(TestEnum.B, record.testEnumUnion);
    Assert.assertEquals(TestEnum.C, record.testEnumArray.get(0));
    Assert.assertEquals(TestEnum.D, record.testEnumUnionArray.get(0));
  }

  @Test(expectedExceptions = FastDeserializerGeneratorException.class, groups = {"deserializationTest"})
  public void shouldNotReadStrippedEnum() throws IOException {
    // given
    Schema oldRecordSchema =
        Schema.parse(this.getClass().getResourceAsStream("/schema/fastserdetestoldextendedenum.avsc"));
    GenericData.Fixed testFixed = newFixed(oldRecordSchema.getField("testFixed").schema(), new byte[]{0x01});
    GenericData.Record subRecord =
        new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema().getTypes().get(1));
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    oldRecord.put("testInt", 1);
    oldRecord.put("testLong", 1l);
    oldRecord.put("testDouble", 1.0);
    oldRecord.put("testFloat", 1.0f);
    oldRecord.put("testBoolean", true);
    oldRecord.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    oldRecord.put("testString", "aaa");
    oldRecord.put("testFixed", testFixed);
    oldRecord.put("testFixedUnion", testFixed);
    oldRecord.put("testFixedArray", Arrays.asList(testFixed));
    oldRecord.put("testFixedUnionArray", Arrays.asList(testFixed));
    oldRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "F"));//new GenericData.EnumSymbol("F"));
    oldRecord.put("testEnumArray", Arrays.asList(
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "F"))); //new GenericData.EnumSymbol("F")));
    oldRecord.put("testEnumUnionArray", Collections.emptyList());

    oldRecord.put("subRecordUnion", subRecord);
    oldRecord.put("subRecord", subRecord);

    oldRecord.put("recordsArray", Collections.emptyList());
    oldRecord.put("recordsArrayMap", Collections.emptyList());
    oldRecord.put("recordsMap", Collections.emptyMap());
    oldRecord.put("recordsMapArray", Collections.emptyMap());

    // when
    TestRecord record = decodeRecordFast(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordField(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    subRecord.subField = "abc";

    record.subRecordUnion = subRecord;
    record.subRecord = subRecord;

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), record.subRecordUnion.subField);
    Assert.assertEquals(new Utf8("abc"), record.subRecord.subField);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordCollectionsField(Boolean whetherUseFastDeserializer) {

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
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), record.recordsArray.get(0).subField);
    Assert.assertEquals(new Utf8("abc"), record.recordsArrayUnion.get(0).subField);
    Assert.assertEquals(new Utf8("abc"), record.recordsMap.get(new Utf8("1")).subField);
    Assert.assertEquals(new Utf8("abc"), record.recordsMapUnion.get(new Utf8("1")).subField);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordComplexCollectionsField(Boolean whetherUseFastDeserializer) {
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
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), record.recordsArrayMap.get(0).get(new Utf8("1")).subField);
    Assert.assertEquals(new Utf8("abc"), record.recordsMapArray.get(new Utf8("1")).get(0).subField);
    Assert.assertEquals(new Utf8("abc"), record.recordsArrayMapUnion.get(0).get(new Utf8("1")).subField);
    Assert.assertEquals(new Utf8("abc"), record.recordsMapArrayUnion.get(new Utf8("1")).get(0).subField);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadAliasedField(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/fastserdetestold.avsc"));
    GenericData.Record subRecord =
        new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema().getTypes().get(1));
    GenericData.EnumSymbol testEnum =
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "A");//new GenericData.EnumSymbol("A");
    GenericData.Fixed testFixed = newFixed(oldRecordSchema.getField("testFixed").schema(), new byte[]{0x01});
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    oldRecord.put("testInt", 1);
    oldRecord.put("testLong", 1l);
    oldRecord.put("testDouble", 1.0);
    oldRecord.put("testFloat", 1.0f);
    oldRecord.put("testBoolean", true);
    oldRecord.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    oldRecord.put("testString", "aaa");
    oldRecord.put("testFixed", testFixed);
    oldRecord.put("testFixedUnion", testFixed);
    oldRecord.put("testFixedArray", Arrays.asList(testFixed));
    oldRecord.put("testFixedUnionArray", Arrays.asList(testFixed));
    oldRecord.put("testEnum", testEnum);
    oldRecord.put("testEnumUnion", testEnum);
    oldRecord.put("testEnumArray", Arrays.asList(testEnum));
    oldRecord.put("testEnumUnionArray", Arrays.asList(testEnum));

    oldRecord.put("subRecordUnion", subRecord);
    oldRecord.put("subRecord", subRecord);

    oldRecord.put("recordsArray", Collections.emptyList());
    oldRecord.put("recordsArrayMap", Collections.emptyList());
    oldRecord.put("recordsMap", Collections.emptyMap());
    oldRecord.put("recordsMapArray", Collections.emptyMap());

    oldRecord.put("testStringAlias", "abc");

    // when
    TestRecord record = null;

    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    }

    // then
    // alias is not well supported in avro-1.4
    if (!Utils.isAvro14()) {
      Assert.assertEquals(new Utf8("abc"), record.testStringUnion);
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldSkipRemovedField(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/fastserdetestold.avsc"));

    GenericData.Record subRecord =
        new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema().getTypes().get(1));
    GenericData.EnumSymbol testEnum =
        AvroCompatibilityHelper.newEnumSymbol(null, "A"); //new GenericData.EnumSymbol("A");
    GenericData.Fixed testFixed = newFixed(oldRecordSchema.getField("testFixed").schema(), new byte[]{0x01});
    GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
    oldRecord.put("testInt", 1);
    oldRecord.put("testLong", 1l);
    oldRecord.put("testDouble", 1.0);
    oldRecord.put("testFloat", 1.0f);
    oldRecord.put("testBoolean", true);
    oldRecord.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    oldRecord.put("testString", "aaa");
    oldRecord.put("testStringAlias", "abc");
    oldRecord.put("removedField", "def");
    oldRecord.put("testFixed", testFixed);
    oldRecord.put("testEnum", testEnum);

    subRecord.put("subField", "abc");
    subRecord.put("removedField", "def");
    subRecord.put("anotherField", "ghi");

    oldRecord.put("subRecordUnion", subRecord);
    oldRecord.put("subRecord", subRecord);
    oldRecord.put("recordsArray", Arrays.asList(subRecord));
    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecord);
    oldRecord.put("recordsMap", recordsMap);

    oldRecord.put("testFixedArray", Collections.emptyList());
    oldRecord.put("testFixedUnionArray", Collections.emptyList());
    oldRecord.put("testEnumArray", Collections.emptyList());
    oldRecord.put("testEnumUnionArray", Collections.emptyList());
    oldRecord.put("recordsArrayMap", Collections.emptyList());
    oldRecord.put("recordsMapArray", Collections.emptyMap());

    // when
    TestRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    }

    // then
    // alias is not well supported in avro-1.4
    if (!Utils.isAvro14()) {
      Assert.assertEquals(new Utf8("abc"), record.testStringUnion);
    }
    Assert.assertEquals(TestEnum.A, record.testEnum);
    Assert.assertEquals(new Utf8("ghi"), record.subRecordUnion.anotherField);
    Assert.assertEquals(new Utf8("ghi"), record.recordsArray.get(0).anotherField);
    Assert.assertEquals(new Utf8("ghi"), record.recordsMap.get(new Utf8("1")).anotherField);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadMultipleChoiceUnion(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    subRecord.subField = "abc";
    record.union = subRecord;

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));

    // then
    Assert.assertEquals(new Utf8("abc"), ((SubRecord) record.union).subField);

    // given
    record.union = "abc";

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), record.union);

    // given
    record.union = 1;

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }
    // then
    Assert.assertEquals(1, record.union);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadArrayOfRecords(Boolean whetherUseFastDeserializer) {
    // given
    Schema arrayRecordSchema = Schema.createArray(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    testRecord.testStringUnion = "abc";

    List<TestRecord> recordsArray = new ArrayList<>();
    recordsArray.add(testRecord);
    recordsArray.add(testRecord);

    // when
    List<TestRecord> array = null;
    if (whetherUseFastDeserializer) {
      array = decodeRecordFast(arrayRecordSchema, arrayRecordSchema,
          specificDataAsDecoder(recordsArray, arrayRecordSchema));
    } else {
      array = decodeRecordSlow(arrayRecordSchema, arrayRecordSchema,
          specificDataAsDecoder(recordsArray, arrayRecordSchema));
    }

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals(new Utf8("abc"), array.get(0).testStringUnion);
    Assert.assertEquals(new Utf8("abc"), array.get(1).testStringUnion);

    // given
    testRecord = emptyTestRecord();
    testRecord.testStringUnion = "abc";

    arrayRecordSchema = Schema.createArray(createUnionSchema(TestRecord.SCHEMA$));

    recordsArray = new ArrayList<>();
    recordsArray.add(testRecord);
    recordsArray.add(testRecord);

    // when
    if (whetherUseFastDeserializer) {
      array = decodeRecordFast(arrayRecordSchema, arrayRecordSchema,
          specificDataAsDecoder(recordsArray, arrayRecordSchema));
    } else {
      array = decodeRecordSlow(arrayRecordSchema, arrayRecordSchema,
          specificDataAsDecoder(recordsArray, arrayRecordSchema));
    }
    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals(new Utf8("abc"), array.get(0).testStringUnion);
    Assert.assertEquals(new Utf8("abc"), array.get(1).testStringUnion);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadMapOfRecords(Boolean whetherUseFastDeserializer) {
    // given
    Schema mapRecordSchema = Schema.createMap(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    testRecord.testStringUnion = "abc";

    Map<String, TestRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", testRecord);
    recordsMap.put("2", testRecord);

    // when
    Map<String, TestRecord> map = null;
    if (whetherUseFastDeserializer) {
      map = decodeRecordFast(mapRecordSchema, mapRecordSchema, specificDataAsDecoder(recordsMap, mapRecordSchema));
    } else {
      map = decodeRecordSlow(mapRecordSchema, mapRecordSchema, specificDataAsDecoder(recordsMap, mapRecordSchema));
    }

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("1")).testStringUnion);
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("2")).testStringUnion);

    // given
    mapRecordSchema = Schema.createMap(FastSerdeTestsSupport.createUnionSchema(TestRecord.SCHEMA$));

    testRecord = emptyTestRecord();
    testRecord.testStringUnion = "abc";

    recordsMap = new HashMap<>();
    recordsMap.put("1", testRecord);
    recordsMap.put("2", testRecord);

    // when
    if (whetherUseFastDeserializer) {
      map = decodeRecordFast(mapRecordSchema, mapRecordSchema, specificDataAsDecoder(recordsMap, mapRecordSchema));
    } else {
      map = decodeRecordSlow(mapRecordSchema, mapRecordSchema, specificDataAsDecoder(recordsMap, mapRecordSchema));
    }
    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("1")).testStringUnion);
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("2")).testStringUnion);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadWithTypesDeletedFromReaderSchema(Boolean whetherUseFastDeserializer) throws IOException {
    // the purpose of this scenario is to test whether types removed in reader schema
    // will not be considered a part of inferred classpath and will not cause the ClassNotFoundException.

    // given
    Schema classpathOldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/classpathOldTest.avsc"));
    Schema classpathFixedSchema = classpathOldRecordSchema.getField("classpathFixed").schema();
    Schema classpathEnumSchema = classpathOldRecordSchema.getField("classpathEnum").schema();
    Schema classpathSubRecordSchema = classpathOldRecordSchema.getField("classpathSubRecord").schema();
    GenericData.Record classpathOldRecord = new GenericData.Record(classpathOldRecordSchema);
    classpathOldRecord.put("field", "abc");

    GenericData.Fixed classpathFixed = newFixed(classpathFixedSchema, new byte[]{0x01});
    Map<String, GenericData.Fixed> classpathFixedMap = new HashMap<>();
    classpathFixedMap.put("key", classpathFixed);
    classpathOldRecord.put("classpathFixed", classpathFixed);
    classpathOldRecord.put("classpathFixedUnion", classpathFixed);
    classpathOldRecord.put("classpathFixedArray", Arrays.asList(classpathFixed));
    classpathOldRecord.put("classpathFixedUnionArray", Arrays.asList(classpathFixed));
    classpathOldRecord.put("classpathFixedMap", classpathFixedMap);
    classpathOldRecord.put("classpathFixedUnionMap", classpathFixedMap);

    GenericData.EnumSymbol classpathEnum = AvroCompatibilityHelper.newEnumSymbol(classpathEnumSchema, "A");
    Map<String, GenericData.EnumSymbol> classpathEnumMap = new HashMap<>();
    classpathEnumMap.put("key", classpathEnum);
    classpathOldRecord.put("classpathEnum", classpathEnum);
    classpathOldRecord.put("classpathEnumUnion", classpathEnum);
    classpathOldRecord.put("classpathEnumArray", Arrays.asList(classpathEnum));
    classpathOldRecord.put("classpathEnumUnionArray", Arrays.asList(classpathEnum));
    classpathOldRecord.put("classpathEnumMap", classpathEnumMap);
    classpathOldRecord.put("classpathEnumUnionMap", classpathEnumMap);

    GenericData.Record classpathSubRecord = new GenericData.Record(classpathSubRecordSchema);
    Map<String, GenericData.Record> classpathSubRecordMap = new HashMap<>();
    classpathSubRecordMap.put("key", classpathSubRecord);
    classpathSubRecord.put("subField", "abc");
    classpathOldRecord.put("classpathSubRecord", classpathSubRecord);
    classpathOldRecord.put("classpathSubRecordUnion", classpathSubRecord);
    classpathOldRecord.put("classpathSubRecordArray", Arrays.asList(classpathSubRecord));
    classpathOldRecord.put("classpathSubRecordUnionArray", Arrays.asList(classpathSubRecord));
    classpathOldRecord.put("classpathSubRecordMap", classpathSubRecordMap);
    classpathOldRecord.put("classpathSubRecordUnionMap", classpathSubRecordMap);

    // when
    ClasspathTestRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(ClasspathTestRecord.SCHEMA$, classpathOldRecordSchema, genericDataAsDecoder(classpathOldRecord));
    } else {
      record = decodeRecordSlow(ClasspathTestRecord.SCHEMA$, classpathOldRecordSchema, genericDataAsDecoder(classpathOldRecord));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), record.field);
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeRecordFast(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastSpecificDeserializerGenerator(writerSchema, readerSchema, tempDir, classLoader,
            null).generateDeserializer();

    try {
      return deserializer.deserialize(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeRecordSlow(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    org.apache.avro.io.DatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
    try {
      return datumReader.read(null, decoder);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
