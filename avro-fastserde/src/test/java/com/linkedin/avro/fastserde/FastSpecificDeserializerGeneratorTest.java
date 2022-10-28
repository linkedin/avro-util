
package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.FullRecord;
import com.linkedin.avro.fastserde.generated.avro.IntRecord;
import com.linkedin.avro.fastserde.generated.avro.MyEnumV2;
import com.linkedin.avro.fastserde.generated.avro.MyRecordV2;
import com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField;
import com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord;
import com.linkedin.avro.fastserde.generated.avro.SplitRecordTest1;
import com.linkedin.avro.fastserde.generated.avro.SplitRecordTest2;
import com.linkedin.avro.fastserde.generated.avro.StringRecord;
import com.linkedin.avro.fastserde.generated.avro.SubRecord;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
import com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField;
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
import java.util.stream.Collectors;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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

  private static final List<TestFixed> EMPTY_TEST_FIXED_LIST = new ArrayList<>();

  @DataProvider(name = "SlowFastDeserializer")
  public static Object[][] deserializers() {
    return new Object[][]{{true}, {false}};
  }

  public static TestRecord emptyTestRecord() {
    TestRecord record = new TestRecord();

    TestFixed testFixed1 = new TestFixed();
    testFixed1.bytes(new byte[]{0x01});
    setField(record, "testFixed", testFixed1);
    setField(record, "testFixedArray", EMPTY_TEST_FIXED_LIST);
    TestFixed testFixed2 = new TestFixed();
    testFixed2.bytes(new byte[]{0x01});
    setField(record, "testFixedUnionArray", Arrays.asList(testFixed2));

    setField(record, "testEnum", TestEnum.A);
    setField(record, "testEnumArray", Collections.emptyList());
    setField(record, "testEnumUnionArray", Arrays.asList(TestEnum.A));
    setField(record, "booleanArray", Collections.emptyList());
    setField(record, "doubleArray", Collections.emptyList());
    setField(record, "floatArray", Collections.emptyList());
    setField(record, "intArray", Collections.emptyList());
    setField(record, "longArray", Collections.emptyList());
    setField(record, "stringArray", Collections.emptyList());
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

  @BeforeTest(groups = {"deserializationTest"})
  public void prepare() throws Exception {
    tempDir = getCodeGenDirectory();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastSpecificDeserializerGeneratorTest.class.getClassLoader());

    // In order to test the functionallity of the record split we set an unusually low number
    FastGenericDeserializerGenerator.setFieldsPerPopulationMethod(2);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadPrimitives(Boolean whetherUseFastDeserializer) {
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
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(1, getField(record, "testInt"));
    Assert.assertEquals(new Integer(1), getField(record, "testIntUnion"));
    Assert.assertEquals(new Utf8("aaa"), getField(record, "testString"));
    Assert.assertEquals(new Utf8("aaa"), getField(record, "testStringUnion"));
    Assert.assertEquals(1l, getField(record, "testLong"));
    Assert.assertEquals(new Long(1), getField(record, "testLongUnion"));
    Assert.assertEquals(1.0, (Double) getField(record, "testDouble"), 0);
    Assert.assertEquals(new Double(1.0), getField(record, "testDoubleUnion"));
    Assert.assertEquals(1.0f, (Float) getField(record, "testFloat"), 0);
    Assert.assertEquals(new Float(1.0f), getField(record, "testFloatUnion"));
    Assert.assertEquals(true, getField(record, "testBoolean"));
    Assert.assertEquals(new Boolean(true), getField(record, "testBooleanUnion"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), getField(record, "testBytes"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), getField(record, "testBytesUnion"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadFixed(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();

    TestFixed testFixed1 = new TestFixed();
    testFixed1.bytes(new byte[]{0x01});
    setField(record, "testFixed", testFixed1);
    TestFixed testFixed2 = new TestFixed();
    testFixed2.bytes(new byte[]{0x02});
    setField(record, "testFixedUnion", testFixed2);
    TestFixed testFixed3 = new TestFixed();
    testFixed3.bytes(new byte[]{0x03});
    TestFixed testFixed4 = new TestFixed();
    testFixed4.bytes(new byte[]{0x04});
    setField(record, "testFixedArray", Arrays.asList(testFixed3));
    setField(record, "testFixedUnionArray", Arrays.asList(testFixed4));

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new byte[]{0x01}, ((TestFixed) getField(record, "testFixed")).bytes());
    Assert.assertEquals(new byte[]{0x02}, ((TestFixed) getField(record, "testFixedUnion")).bytes());
    Assert.assertEquals(new byte[]{0x03}, ((List<TestFixed>) getField(record, "testFixedArray")).get(0).bytes());
    Assert.assertEquals(new byte[]{0x04}, ((List<TestFixed>) getField(record, "testFixedUnionArray")).get(0).bytes());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadEnum(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();

    setField(record, "testEnum", TestEnum.A);
    setField(record, "testEnumUnion", TestEnum.A);
    setField(record, "testEnumArray", Arrays.asList(TestEnum.A));
    setField(record, "testEnumUnionArray", Arrays.asList(TestEnum.A));

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(TestEnum.A, getField(record, "testEnum"));
    Assert.assertEquals(TestEnum.A, getField(record, "testEnumUnion"));
    Assert.assertEquals(TestEnum.A, ((List<TestEnum>) getField(record, "testEnumArray")).get(0));
    Assert.assertEquals(TestEnum.A, ((List<TestEnum>) getField(record, "testEnumUnionArray")).get(0));
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
    Assert.assertEquals(TestEnum.A, getField(record, "testEnum"));
    Assert.assertEquals(TestEnum.B, getField(record, "testEnumUnion"));
    Assert.assertEquals(TestEnum.C, ((List<TestEnum>) getField(record, "testEnumArray")).get(0));
    Assert.assertEquals(TestEnum.D, ((List<TestEnum>) getField(record, "testEnumUnionArray")).get(0));
  }

  @Test(expectedExceptions = AvroTypeException.class, groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldDecodeRecordAndOnlyFailWhenReadingStrippedEnum(Boolean whetherUseFastDeserializer)
      throws IOException {
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
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "F", false));//new GenericData.EnumSymbol("F"));
    oldRecord.put("testEnumArray", Arrays.asList(
        AvroCompatibilityHelper.newEnumSymbol(SCHEMA_FOR_TEST_ENUM, "F", false))); //new GenericData.EnumSymbol("F")));
    oldRecord.put("testEnumUnionArray", Collections.emptyList());

    oldRecord.put("subRecordUnion", subRecord);
    oldRecord.put("subRecord", subRecord);

    oldRecord.put("recordsArray", Collections.emptyList());
    oldRecord.put("recordsArrayMap", Collections.emptyList());
    oldRecord.put("recordsMap", Collections.emptyMap());
    oldRecord.put("recordsMapArray", Collections.emptyMap());

    // when
    if (whetherUseFastDeserializer) {
      decodeRecordFast(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    } else {
      decodeRecordSlow(TestRecord.SCHEMA$, oldRecordSchema, genericDataAsDecoder(oldRecord));
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordField(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    setField(subRecord, "subField", "abc");

    setField(record, "subRecordUnion", subRecord);
    setField(record, "subRecord", subRecord);

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), getField((SubRecord) getField(record, "subRecordUnion"), "subField"));
    Assert.assertEquals(new Utf8("abc"), getField((SubRecord) getField(record, "subRecord"), "subField"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordCollectionsField(Boolean whetherUseFastDeserializer) {

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
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), getField(((List<SubRecord>) getField(record, "recordsArray")).get(0), "subField"));
    Assert.assertEquals(new Utf8("abc"), getField(((List<SubRecord>) getField(record, "recordsArrayUnion")).get(0), "subField"));
    Assert.assertEquals(new Utf8("abc"), getField(((Map<CharSequence, SubRecord>) getField(record, "recordsMap")).get(new Utf8("1")), "subField"));
    Assert.assertEquals(new Utf8("abc"), getField(((Map<CharSequence, SubRecord>) getField(record, "recordsMapUnion")).get(new Utf8("1")), "subField"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSubRecordComplexCollectionsField(Boolean whetherUseFastDeserializer) {
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
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), getField(((List<Map<CharSequence, SubRecord>>) getField(record, "recordsArrayMap")).get(0).get(new Utf8("1")), "subField"));
    Assert.assertEquals(new Utf8("abc"), getField(((Map<CharSequence, List<SubRecord>>) getField(record, "recordsMapArray")).get(new Utf8("1")).get(0), "subField"));
    Assert.assertEquals(new Utf8("abc"), getField(((List<Map<CharSequence, SubRecord>>) getField(record, "recordsArrayMapUnion")).get(0).get(new Utf8("1")), "subField"));
    Assert.assertEquals(new Utf8("abc"), getField(((Map<CharSequence, List<SubRecord>>) getField(record, "recordsMapArrayUnion")).get(new Utf8("1")).get(0), "subField"));
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
      Assert.assertEquals(new Utf8("abc"), getField(record, "testStringUnion"));
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldSkipRemovedField(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema oldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/fastserdetestold.avsc"));

    GenericData.Record subRecord =
        new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema().getTypes().get(1));
    GenericData.EnumSymbol testEnum = AvroCompatibilityHelper.newEnumSymbol(
            oldRecordSchema.getField("testEnum").schema(),
            "A"
    ); //new GenericData.EnumSymbol("A");
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
      Assert.assertEquals(new Utf8("abc"), getField(record, "testStringUnion"));
    }
    Assert.assertEquals(TestEnum.A, getField(record, "testEnum"));
    Assert.assertEquals(new Utf8("ghi"), getField((SubRecord) getField(record, "subRecordUnion"), "anotherField"));
    Assert.assertEquals(new Utf8("ghi"), getField(((List<SubRecord>) getField(record, "recordsArray")).get(0), "anotherField"));
    Assert.assertEquals(new Utf8("ghi"), getField(((Map<CharSequence, SubRecord>) getField(record, "recordsMap")).get(new Utf8("1")), "anotherField"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadMultipleChoiceUnion(Boolean whetherUseFastDeserializer) {
    // given
    TestRecord record = emptyTestRecord();
    SubRecord subRecord = new SubRecord();
    setField(subRecord, "subField", "abc");
    setField(record, "union", subRecord);

    // when
    record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));

    // then
    Assert.assertEquals(new Utf8("abc"), getField((SubRecord) getField(record, "union"), "subField"));

    // given
    setField(record, "union", "abc");

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), getField(record, "union"));

    // given
    setField(record, "union", 1);

    // when
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    } else {
      record = decodeRecordSlow(TestRecord.SCHEMA$, TestRecord.SCHEMA$, specificDataAsDecoder(record));
    }
    // then
    Assert.assertEquals(1, getField(record, "union"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadArrayOfRecords(Boolean whetherUseFastDeserializer) {
    // given
    Schema arrayRecordSchema = Schema.createArray(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    setField(testRecord, "testStringUnion", "abc");

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
    Assert.assertEquals(new Utf8("abc"), getField(array.get(0), "testStringUnion"));
    Assert.assertEquals(new Utf8("abc"), getField(array.get(1), "testStringUnion"));

    // given
    testRecord = emptyTestRecord();
    setField(testRecord, "testStringUnion", "abc");

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
    Assert.assertEquals(new Utf8("abc"), getField(array.get(0), "testStringUnion"));
    Assert.assertEquals(new Utf8("abc"), getField(array.get(1), "testStringUnion"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadMapOfRecords(Boolean whetherUseFastDeserializer) {
    // given
    Schema mapRecordSchema = Schema.createMap(TestRecord.SCHEMA$);

    TestRecord testRecord = emptyTestRecord();
    setField(testRecord, "testStringUnion", "abc");

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
    Assert.assertEquals(new Utf8("abc"), getField(map.get(new Utf8("1")), "testStringUnion"));
    Assert.assertEquals(new Utf8("abc"), getField(map.get(new Utf8("2")), "testStringUnion"));

    // given
    mapRecordSchema = Schema.createMap(FastSerdeTestsSupport.createUnionSchema(TestRecord.SCHEMA$));

    testRecord = emptyTestRecord();
    setField(testRecord, "testStringUnion", "abc");

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
    Assert.assertEquals(new Utf8("abc"), getField(map.get(new Utf8("1")), "testStringUnion"));
    Assert.assertEquals(new Utf8("abc"), getField(map.get(new Utf8("2")), "testStringUnion"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadWithTypesDeletedFromReaderSchema(Boolean whetherUseFastDeserializer) throws IOException {
    // given
    Schema removedTypesOldRecordSchema = Schema.parse(this.getClass().getResourceAsStream("/schema/removedTypesOldTest.avsc"));
    Schema removedFixedSchema = removedTypesOldRecordSchema.getField("removedFixed").schema();
    Schema removedEnumSchema = removedTypesOldRecordSchema.getField("removedEnum").schema();
    Schema removedSubRecordSchema = removedTypesOldRecordSchema.getField("removedSubRecord").schema();
    GenericData.Record removedOldRecord = new GenericData.Record(removedTypesOldRecordSchema);
    removedOldRecord.put("field", "abc");

    ByteBuffer removedBytes = ByteBuffer.wrap(new byte[]{0x01});
    Map<String, ByteBuffer> removedBytesMap = new HashMap<>();
    removedOldRecord.put("removedBytes", removedBytes);
    removedOldRecord.put("removedBytesUnion", removedBytes);
    removedOldRecord.put("removedBytesArray", Arrays.asList(removedBytes));
    removedOldRecord.put("removedBytesMap", removedBytesMap);
    removedOldRecord.put("removedBytesUnionArray", Arrays.asList(removedBytes));
    removedOldRecord.put("removedBytesUnionMap", removedBytesMap);

    GenericData.Fixed removedFixed = newFixed(removedFixedSchema, new byte[]{0x01});
    Map<String, GenericData.Fixed> removedFixedMap = new HashMap<>();
    removedFixedMap.put("key", removedFixed);
    removedOldRecord.put("removedFixed", removedFixed);
    removedOldRecord.put("removedFixedUnion", removedFixed);
    removedOldRecord.put("removedFixedArray", Arrays.asList(removedFixed));
    removedOldRecord.put("removedFixedUnionArray", Arrays.asList(removedFixed));
    removedOldRecord.put("removedFixedMap", removedFixedMap);
    removedOldRecord.put("removedFixedUnionMap", removedFixedMap);

    GenericData.EnumSymbol removedEnum = AvroCompatibilityHelper.newEnumSymbol(removedEnumSchema, "A");
    Map<String, GenericData.EnumSymbol> removedEnumMap = new HashMap<>();
    removedEnumMap.put("key", removedEnum);
    removedOldRecord.put("removedEnum", removedEnum);
    removedOldRecord.put("removedEnumUnion", removedEnum);
    removedOldRecord.put("removedEnumArray", Arrays.asList(removedEnum));
    removedOldRecord.put("removedEnumUnionArray", Arrays.asList(removedEnum));
    removedOldRecord.put("removedEnumMap", removedEnumMap);
    removedOldRecord.put("removedEnumUnionMap", removedEnumMap);

    GenericData.Record removedSubRecord = new GenericData.Record(removedSubRecordSchema);
    Map<String, GenericData.Record> removedSubRecordMap = new HashMap<>();
    removedSubRecordMap.put("key", removedSubRecord);
    removedSubRecord.put("subField", "abc");
    removedOldRecord.put("removedSubRecord", removedSubRecord);
    removedOldRecord.put("removedSubRecordUnion", removedSubRecord);
    removedOldRecord.put("removedSubRecordArray", Arrays.asList(removedSubRecord));
    removedOldRecord.put("removedSubRecordUnionArray", Arrays.asList(removedSubRecord));
    removedOldRecord.put("removedSubRecordMap", removedSubRecordMap);
    removedOldRecord.put("removedSubRecordUnionMap", removedSubRecordMap);

    // when
    RemovedTypesTestRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(RemovedTypesTestRecord.SCHEMA$, removedTypesOldRecordSchema, genericDataAsDecoder(removedOldRecord));
    } else {
      record = decodeRecordSlow(RemovedTypesTestRecord.SCHEMA$, removedTypesOldRecordSchema, genericDataAsDecoder(removedOldRecord));
    }

    // then
    Assert.assertEquals(new Utf8("abc"), getField(record, "field"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void testDeserializerWithDifferentNamespace(Boolean whetherUseFastDeserializer) throws IOException {
    Schema writerSchema = Schema.parse(this.getClass().getResourceAsStream(
        "/schema/removedTypesTestWithADifferentNamespace.avsc"));

    Assert.assertNotEquals(writerSchema.getNamespace(), RemovedTypesTestRecord.SCHEMA$.getNamespace(),
        "Namespace should be different between writer schema and reader schema");
    GenericRecord object = new GenericData.Record(writerSchema);
    object.put("field", "abc");

    RemovedTypesTestRecord record = null;
    if (whetherUseFastDeserializer) {
      record = decodeRecordFast(RemovedTypesTestRecord.SCHEMA$, writerSchema, genericDataAsDecoder(object));
    } else {
      record = decodeRecordSlow(RemovedTypesTestRecord.SCHEMA$, writerSchema, genericDataAsDecoder(object));
    }
    Assert.assertEquals(getField(record, "field").toString(), "abc");
  }


  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadSplitRecords(Boolean whetherUseFastDeserializer) {
    // alias is not well supported in avro-1.4
    if (!Utils.isAvro14()) {
      // given
      Schema writerSchema = SplitRecordTest1.SCHEMA$;
      Schema readerSchema = SplitRecordTest2.SCHEMA$;

      SplitRecordTest1 splitRecordTest1 = new SplitRecordTest1();

      FullRecord fullRecord1 = new FullRecord();
      setField(fullRecord1, "field1", "test1");
      setField(fullRecord1, "field2", 1);

      FullRecord fullRecord2 = new FullRecord();
      setField(fullRecord2, "field1", "test2");
      setField(fullRecord2, "field2", 2);

      FullRecord fullRecord3 = new FullRecord();
      setField(fullRecord3, "field1", "test3");
      setField(fullRecord3, "field2", 3);

      setField(splitRecordTest1, "record1", fullRecord1);
      setField(splitRecordTest1, "record2", fullRecord2);
      setField(splitRecordTest1, "record3", Collections.singletonList(fullRecord3));

      // when
      SplitRecordTest2 splitRecordTest2;

      if (whetherUseFastDeserializer) {
        splitRecordTest2 = decodeRecordFast(readerSchema, writerSchema, specificDataAsDecoder(splitRecordTest1));
      } else {
        splitRecordTest2 = decodeRecordSlow(readerSchema, writerSchema, specificDataAsDecoder(splitRecordTest1));
      }

      // then
      Assert.assertEquals(getField((StringRecord) getField(splitRecordTest2, "record1"), "field1"), new Utf8("test1"));
      Assert.assertEquals(getField((IntRecord) getField(splitRecordTest2, "record2"), "field2"), 2);
      Assert.assertEquals(getField(((List<FullRecord>) getField(splitRecordTest2, "record3")).get(0), "field1"), new Utf8("test3"));
      Assert.assertEquals(getField(((List<FullRecord>) getField(splitRecordTest2, "record3")).get(0), "field2"), 3);
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadMergedRecords(Boolean whetherUseFastDeserializer) {
    // alias is not well supported in avro-1.4
    if (!Utils.isAvro14()) {
      // given
      Schema writerSchema = SplitRecordTest2.SCHEMA$;
      Schema readerSchema = SplitRecordTest1.SCHEMA$;
      SplitRecordTest2 splitRecordTest2 = new SplitRecordTest2();

      StringRecord stringRecord = new StringRecord();
      setField(stringRecord, "field1", "test1");

      IntRecord intRecord = new IntRecord();
      setField(intRecord, "field2", 2);

      FullRecord fullRecord = new FullRecord();
      setField(fullRecord, "field1", "test3");
      setField(fullRecord, "field2", 3);

      setField(splitRecordTest2, "record1", stringRecord);
      setField(splitRecordTest2, "record2", intRecord);
      setField(splitRecordTest2, "record3", Collections.singletonList(fullRecord));

      // when
      SplitRecordTest1 splitRecordTest1;
      if (whetherUseFastDeserializer) {
        splitRecordTest1 = decodeRecordFast(readerSchema, writerSchema, specificDataAsDecoder(splitRecordTest2));
      } else {
        splitRecordTest1 = decodeRecordSlow(readerSchema, writerSchema, specificDataAsDecoder(splitRecordTest2));
      }

      // then
      Assert.assertEquals(getField((FullRecord) getField(splitRecordTest1, "record1"), "field1"), new Utf8("test1"));
      Assert.assertEquals(getField((FullRecord) getField(splitRecordTest1, "record2"), "field2"), 2);
      Assert.assertEquals(getField(((List<FullRecord>) getField(splitRecordTest1, "record3")).get(0), "field1"), new Utf8("test3"));
      Assert.assertEquals(getField(((List<FullRecord>) getField(splitRecordTest1, "record3")).get(0), "field2"), 3);
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void shouldReadUnionOfRecordFieldsWithSameEnumFieldName(Boolean whetherUseFastDeserializer) {
    Schema writerSchema = UnionOfRecordsWithSameNameEnumField.SCHEMA$;

    UnionOfRecordsWithSameNameEnumField testRecord = new UnionOfRecordsWithSameNameEnumField();
    MyRecordV2 myRecord = new MyRecordV2();
    setField(myRecord, "enumField", MyEnumV2.VAL3);
    setField(testRecord, "unionField", myRecord);

    UnionOfRecordsWithSameNameEnumField deserRecord = null;
    if (whetherUseFastDeserializer) {
      deserRecord = decodeRecordFast(writerSchema, writerSchema, specificDataAsDecoder(testRecord));
    } else {
      deserRecord = decodeRecordSlow(writerSchema, writerSchema, specificDataAsDecoder(testRecord));
    }

    Assert.assertEquals(getField((MyRecordV2) getField(deserRecord, "unionField"), "enumField"), MyEnumV2.VAL3);
  }

  @Test(groups = {"deserializationTest"})
  public void largeSchemasWithUnionCanBeHandled() {
    // doesn't contain "bytes" type in the union field
    Schema readerSchema = AvroCompatibilityHelper.parse("{\"type\":\"record\",\"name\":\"RecordWithLargeUnionField\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"fields\":[{\"name\":\"unionField\",\"type\":[\"string\",\"int\"],\"default\":\"more than 50 letters aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}]}");
    Schema writerSchema = RecordWithLargeUnionField.SCHEMA$;

    FastDeserializerGenerator.MAX_LENGTH_OF_STRING_LITERAL = 5;

    Assert.assertTrue(writerSchema.toString().length() > FastDeserializerGenerator.MAX_LENGTH_OF_STRING_LITERAL);

    // generateDeserializer should not throw an exception
    try {
      new FastSpecificDeserializerGenerator<>(writerSchema, readerSchema, tempDir, classLoader, null).generateDeserializer();
    } catch (Exception e) {
      Assert.fail("Exception was thrown: ", e);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T decodeRecordFast(Schema readerSchema, Schema writerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastSpecificDeserializerGenerator(writerSchema, readerSchema, tempDir, classLoader,
            null).generateDeserializer();

    try {
      return deserializer.deserialize(null, decoder);
    } catch (AvroTypeException e) {
      throw e;
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
