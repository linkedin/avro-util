package com.linkedin.avro.fastserde;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveDoubleList;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastGenericDeserializerGeneratorTest {

  private static File tempDir;
  private static ClassLoader classLoader;

  enum Implementation {
    VANILLA_AVRO(false, FastGenericDeserializerGeneratorTest::decodeRecordSlow),
    COLD_FAST_AVRO(true, FastGenericDeserializerGeneratorTest::decodeRecordColdFast),
    WARM_FAST_AVRO(true, FastGenericDeserializerGeneratorTest::decodeRecordWarmFast);

    boolean isFast;
    DecodeFunction decodeFunction;
    Implementation(boolean isFast, DecodeFunction decodeFunction) {
      this.isFast = isFast;
      this.decodeFunction = decodeFunction;
    }

    interface DecodeFunction<T> {
      T decode(Schema writerSchema, Schema readerSchema, Decoder decoder);
    }

    <T> T decode(Schema writerSchema, Schema readerSchema, Decoder decoder) {
      return (T) decodeFunction.decode(writerSchema, readerSchema, decoder);
    }
  }

  @DataProvider(name = "Implementation")
  public static Object[][] implementations() {
    return new Object[][]{
        {Implementation.VANILLA_AVRO},
        {Implementation.COLD_FAST_AVRO},
        {Implementation.WARM_FAST_AVRO}
    };
  }

  /**
   * @deprecated TODO Migrate to {@link #implementations()}
   */
  @Deprecated
  @DataProvider(name = "SlowFastDeserializer")
  public static Object[][] deserializers() {
    return new Object[][]{{true}, {false}};
  }

  @BeforeTest(groups = {"deserializationTest"})
  public void prepare() throws Exception {
    tempDir = getCodeGenDirectory();

    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastGenericDeserializerGeneratorTest.class.getClassLoader());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadPrimitives(Implementation implementation) {
    // given
    Schema recordSchema = createRecord(
        createField("testInt", Schema.create(Schema.Type.INT)),
        createPrimitiveUnionFieldSchema("testIntUnion", Schema.Type.INT),
        createField("testString", Schema.create(Schema.Type.STRING)),
        createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING),
        createField("testLong", Schema.create(Schema.Type.LONG)),
        createPrimitiveUnionFieldSchema("testLongUnion", Schema.Type.LONG),
        createField("testDouble", Schema.create(Schema.Type.DOUBLE)),
        createPrimitiveUnionFieldSchema("testDoubleUnion", Schema.Type.DOUBLE),
        createField("testFloat", Schema.create(Schema.Type.FLOAT)),
        createPrimitiveUnionFieldSchema("testFloatUnion", Schema.Type.FLOAT),
        createField("testBoolean", Schema.create(Schema.Type.BOOLEAN)),
        createPrimitiveUnionFieldSchema("testBooleanUnion", Schema.Type.BOOLEAN),
        createField("testBytes", Schema.create(Schema.Type.BYTES)),
        createPrimitiveUnionFieldSchema("testBytesUnion", Schema.Type.BYTES));

    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("testInt", 1);
    record.put("testIntUnion", 1);
    record.put("testString", "aaa");
    record.put("testStringUnion", "aaa");
    record.put("testLong", 1l);
    record.put("testLongUnion", 1l);
    record.put("testDouble", 1.0);
    record.put("testDoubleUnion", 1.0);
    record.put("testFloat", 1.0f);
    record.put("testFloatUnion", 1.0f);
    record.put("testBoolean", true);
    record.put("testBooleanUnion", true);
    record.put("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
    record.put("testBytesUnion", ByteBuffer.wrap(new byte[]{0x01, 0x02}));

    // when
    GenericRecord decodedRecord = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(1, decodedRecord.get("testInt"));
    Assert.assertEquals(1, decodedRecord.get("testIntUnion"));
    Assert.assertEquals(new Utf8("aaa"), decodedRecord.get("testString"));
    Assert.assertEquals(new Utf8("aaa"), decodedRecord.get("testStringUnion"));
    Assert.assertEquals(1l, decodedRecord.get("testLong"));
    Assert.assertEquals(1l, decodedRecord.get("testLongUnion"));
    Assert.assertEquals(1.0, decodedRecord.get("testDouble"));
    Assert.assertEquals(1.0, decodedRecord.get("testDoubleUnion"));
    Assert.assertEquals(1.0f, decodedRecord.get("testFloat"));
    Assert.assertEquals(1.0f, decodedRecord.get("testFloatUnion"));
    Assert.assertEquals(true, decodedRecord.get("testBoolean"));
    Assert.assertEquals(true, decodedRecord.get("testBooleanUnion"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), decodedRecord.get("testBytes"));
    Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), decodedRecord.get("testBytesUnion"));
  }

  public GenericData.Fixed newFixed(Schema fixedSchema, byte[] bytes) {
    GenericData.Fixed fixed = new GenericData.Fixed(fixedSchema);
    fixed.bytes(bytes);
    return fixed;
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadFixed(Implementation implementation) {
    // given
    Schema fixedSchema = createFixedSchema("testFixed", 2);
    Schema recordSchema = createRecord(
        createField("testFixed", fixedSchema),
        createUnionFieldWithNull("testFixedUnion", fixedSchema),
        createArrayFieldSchema("testFixedArray", fixedSchema),
        createArrayFieldSchema("testFixedUnionArray", createUnionSchema(fixedSchema)));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testFixed", newFixed(fixedSchema, new byte[]{0x01, 0x02}));
    originalRecord.put("testFixedUnion", newFixed(fixedSchema, new byte[]{0x03, 0x04}));
    originalRecord.put("testFixedArray", Arrays.asList(newFixed(fixedSchema, new byte[]{0x05, 0x06})));
    originalRecord.put("testFixedUnionArray", Arrays.asList(newFixed(fixedSchema, new byte[]{0x07, 0x08})));

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(originalRecord));

    // then
    Assert.assertEquals(new byte[]{0x01, 0x02}, ((GenericData.Fixed) record.get("testFixed")).bytes());
    Assert.assertEquals(new byte[]{0x03, 0x04}, ((GenericData.Fixed) record.get("testFixedUnion")).bytes());
    Assert.assertEquals(new byte[]{0x05, 0x06},
        ((List<GenericData.Fixed>) record.get("testFixedArray")).get(0).bytes());
    Assert.assertEquals(new byte[]{0x07, 0x08},
        ((List<GenericData.Fixed>) record.get("testFixedUnionArray")).get(0).bytes());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadEnum(Implementation implementation) {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema = createRecord(
        createField("testEnum", enumSchema),
        createUnionFieldWithNull("testEnumUnion", enumSchema),
        createArrayFieldSchema("testEnumArray", enumSchema),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"));//new GenericData.EnumSymbol("A"));
    originalRecord.put("testEnumUnion",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"));//new GenericData.EnumSymbol("A"));
    originalRecord.put("testEnumArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A")));//new GenericData.EnumSymbol("A")));
    originalRecord.put("testEnumUnionArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A")));//new GenericData.EnumSymbol("A")));

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(originalRecord));

    // then
    Assert.assertEquals("A", record.get("testEnum").toString());
    Assert.assertEquals("A", record.get("testEnumUnion").toString());
    Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
    Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadPermutatedEnum(Implementation implementation) {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C", "D", "E"});
    Schema recordSchema = createRecord(
        createField("testEnum", enumSchema),
        createUnionFieldWithNull("testEnumUnion", enumSchema),
        createArrayFieldSchema("testEnumArray", enumSchema),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"));//new GenericData.EnumSymbol("A"));
    originalRecord.put("testEnumUnion",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "B"));//new GenericData.EnumSymbol("B"));
    originalRecord.put("testEnumArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "C")));//new GenericData.EnumSymbol("C")));
    originalRecord.put("testEnumUnionArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "D")));//new GenericData.EnumSymbol("D")));

    Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"B", "A", "D", "E", "C"});
    Schema recordSchema1 = createRecord(
        createField("testEnum", enumSchema1),
        createUnionFieldWithNull("testEnumUnion", enumSchema1),
        createArrayFieldSchema("testEnumArray", enumSchema1),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema1)));

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema1, genericDataAsDecoder(originalRecord));

    // then
    Assert.assertEquals("A", record.get("testEnum").toString());
    Assert.assertEquals("B", record.get("testEnumUnion").toString());
    Assert.assertEquals("C", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
    Assert.assertEquals("D", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
  }

  @Test(expectedExceptions = FastDeserializerGeneratorException.class, groups = {"deserializationTest"})
  public void shouldNotReadStrippedEnum() {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C"});
    Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(null, "C"));//new GenericData.EnumSymbol("C"));

    Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1));

    // when
    GenericRecord record = decodeRecordWarmFast(recordSchema, recordSchema1, genericDataAsDecoder(originalRecord));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadSubRecordField(Implementation implementation) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema = createRecord(
        createUnionFieldWithNull("record", subRecordSchema),
        createField("record1", subRecordSchema),
        createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    GenericRecord subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericRecord builder = new GenericData.Record(recordSchema);
    builder.put("record", subRecordBuilder);
    builder.put("record1", subRecordBuilder);
    builder.put("field", "abc");

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("record")).get("subField"));
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record")).getSchema().hashCode());
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("record1")).get("subField"));
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record1")).getSchema().hashCode());
    Assert.assertEquals(new Utf8("abc"), record.get("field"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadSubRecordCollectionsField(Implementation implementation) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord(
        createArrayFieldSchema("recordsArray", subRecordSchema),
        createMapFieldSchema("recordsMap", subRecordSchema),
        createUnionFieldWithNull("recordsArrayUnion", Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionFieldWithNull("recordsMapUnion", Schema.createMap(createUnionSchema(subRecordSchema))));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericRecord builder = new GenericData.Record(recordSchema);
    List<GenericData.Record> recordsArray = new ArrayList<>();
    recordsArray.add(subRecordBuilder);
    builder.put("recordsArray", recordsArray);
    builder.put("recordsArrayUnion", recordsArray);
    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    builder.put("recordsMap", recordsMap);
    builder.put("recordsMapUnion", recordsMap);

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"),
        ((List<GenericData.Record>) record.get("recordsArray")).get(0).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((List<GenericData.Record>) record.get("recordsArrayUnion")).get(0).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, GenericData.Record>) record.get("recordsMap")).get(new Utf8("1")).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, GenericData.Record>) record.get("recordsMapUnion")).get(new Utf8("1")).get("subField"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadSubRecordComplexCollectionsField(Implementation implementation) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    Schema recordSchema = createRecord(
        createArrayFieldSchema("recordsArrayMap", Schema.createMap(createUnionSchema(subRecordSchema))),
        createMapFieldSchema("recordsMapArray", Schema.createArray(createUnionSchema(subRecordSchema))),
        createUnionFieldWithNull("recordsArrayMapUnion",
            Schema.createArray(Schema.createMap(createUnionSchema(subRecordSchema)))),
        createUnionFieldWithNull("recordsMapArrayUnion",
            Schema.createMap(Schema.createArray(createUnionSchema(subRecordSchema)))));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericData.Record builder = new GenericData.Record(recordSchema);
    List<Map<String, GenericRecord>> recordsArrayMap = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new HashMap<>();
    recordMap.put("1", subRecordBuilder);
    recordsArrayMap.add(recordMap);

    builder.put("recordsArrayMap", recordsArrayMap);
    builder.put("recordsArrayMapUnion", recordsArrayMap);

    Map<String, List<GenericRecord>> recordsMapArray = new HashMap<>();
    List<GenericRecord> recordList = new ArrayList<>();
    recordList.add(subRecordBuilder);
    recordsMapArray.put("1", recordList);

    builder.put("recordsMapArray", recordsMapArray);
    builder.put("recordsMapArrayUnion", recordsMapArray);

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"),
        ((List<Map<String, GenericRecord>>) record.get("recordsArrayMap")).get(0).get(new Utf8("1")).get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, List<GenericRecord>>) record.get("recordsMapArray")).get(new Utf8("1")).get(0).get("subField"));
    Assert.assertEquals(new Utf8("abc"), ((List<Map<String, GenericRecord>>) record.get("recordsArrayMapUnion")).get(0)
        .get(new Utf8("1"))
        .get("subField"));
    Assert.assertEquals(new Utf8("abc"),
        ((Map<String, List<GenericRecord>>) record.get("recordsMapArrayUnion")).get(new Utf8("1"))
            .get(0)
            .get("subField"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadAliasedField(Implementation implementation) {
    if (Utils.isAvro14()) {
      /**
       * The rest of the code in this function has been "adapted" to work with 1.4's bugs, but the end result is so
       * contrived that it's probably better to not run this at all. Feel free to comment the Skip and try it out...
       */
      throw new SkipException("Aliases are not properly supported in Avro 1.4");
    }

    // given
    String field1Name = "testString";
    String field1Value = "abc";
    /** A Supplier is needed because Avro doesn't tolerate reusing {@link Field} instances in multiple {@link Schema}... */
    Supplier<Schema.Field> field1Supplier = () -> createPrimitiveUnionFieldSchema(field1Name, Schema.Type.STRING);
    /**
     * Avro 1.4's support for aliases is so broken that if any of the fields have an alias, then ALL fields must have one.
     * Therefore, we are "fixing" the new schema in this weird way by having an alias which is the same as the original
     * name, otherwise that field is completely gone when decoding. This is due to a bug in the 1.4 implementation of
     * {@link Schema#getFieldAlias(Schema.Name, String, Map)}.
     */
    Schema.Field newField1 = Utils.isAvro14()
        ? addAliases(createPrimitiveUnionFieldSchema(field1Name, Schema.Type.STRING), field1Name)
        : field1Supplier.get();

    String originalField2Name = "testStringUnion";
    String newField2Name = "testStringUnionAlias";
    String field2Value = "def";
    Schema.Field originalField2 = createPrimitiveUnionFieldSchema(originalField2Name, Schema.Type.STRING);
    Schema.Field newField2 = addAliases(createPrimitiveUnionFieldSchema(newField2Name, Schema.Type.STRING), originalField2Name);

    Schema record1Schema = createRecord(field1Supplier.get(), originalField2);
    Schema record2Schema = createRecord(newField1, newField2);

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put(field1Name, field1Value);
    builder.put(originalField2Name, field2Value);

    // when
    GenericRecord record = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(record.get(field1Name), new Utf8(field1Value));
    if (!Utils.isAvro14()) {
      Assert.assertEquals(record.get(newField2Name), new Utf8(field2Value));
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldSkipRemovedField(Implementation implementation) {
    // given
    Schema subRecord1Schema = createRecord("subRecord",
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
    Schema record1Schema = createRecord(
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
        createUnionFieldWithNull("subRecord", subRecord1Schema),
        createMapFieldSchema("subRecordMap", subRecord1Schema),
        createArrayFieldSchema("subRecordArray", subRecord1Schema));
    Schema subRecord2Schema = createRecord("subRecord",
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
    Schema record2Schema = createRecord(
        createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
        createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
        createUnionFieldWithNull("subRecord", subRecord2Schema),
        createMapFieldSchema("subRecordMap", subRecord2Schema),
        createArrayFieldSchema("subRecordArray", subRecord2Schema));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecord1Schema);
    subRecordBuilder.put("testNotRemoved", "abc");
    subRecordBuilder.put("testRemoved", "def");
    subRecordBuilder.put("testNotRemoved2", "ghi");

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put("testNotRemoved", "abc");
    builder.put("testRemoved", "def");
    builder.put("testNotRemoved2", "ghi");
    builder.put("subRecord", subRecordBuilder);
    builder.put("subRecordArray", Arrays.asList(subRecordBuilder));

    Map<String, GenericRecord> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    builder.put("subRecordMap", recordsMap);

    // when
    GenericRecord record = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"), record.get("testNotRemoved"));
    Assert.assertNull(record.get("testRemoved"));
    Assert.assertEquals(new Utf8("ghi"), record.get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"), ((GenericRecord) record.get("subRecord")).get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"),
        ((List<GenericRecord>) record.get("subRecordArray")).get(0).get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"),
        ((Map<String, GenericRecord>) record.get("subRecordMap")).get(new Utf8("1")).get("testNotRemoved2"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldTolerateUnionReorderingThatIncludeString(Implementation implementation) {
    // given
    Schema record1Schema = createRecord(
        createPrimitiveUnionFieldSchema("test", Schema.Type.STRING, Schema.Type.INT));
    Schema record2Schema = createRecord(
        createPrimitiveUnionFieldSchema("test", Schema.Type.INT, Schema.Type.STRING));

    GenericData.Record builderForSchema1WithString = new GenericData.Record(record1Schema);
    builderForSchema1WithString.put("test", "abc");
    GenericData.Record builderForSchema1WithInt = new GenericData.Record(record1Schema);
    builderForSchema1WithInt.put("test", 1);
    GenericData.Record builderForSchema2WithString = new GenericData.Record(record2Schema);
    builderForSchema2WithString.put("test", "abc");
    GenericData.Record builderForSchema2WithInt = new GenericData.Record(record2Schema);
    builderForSchema2WithInt.put("test", 1);

    // when

    // Evolution:
    GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithString));
    try {
      GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithInt));
    } catch (Exception e) {
      // broken
    }
    GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithString));
    try {
      GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithInt));
    } catch (Exception e) {
      // broken
    }

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithString));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithString));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithInt));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithInt));

    // then

    // Evolution:
    Assert.assertEquals(recordA.get("test"), new Utf8("abc"));
//    Assert.assertEquals(recordB.get("test"), 1);
    Assert.assertEquals(recordC.get("test"), new Utf8("abc"));
//    Assert.assertEquals(recordD.get("test"), 1);

    // Non-evolution
    Assert.assertEquals(recordE.get("test"), new Utf8("abc"));
    Assert.assertEquals(recordF.get("test"), new Utf8("abc"));
    Assert.assertEquals(recordG.get("test"), 1);
    Assert.assertEquals(recordH.get("test"), 1);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldTolerateUnionReorderingWithNonString(Implementation implementation) {
    // given
    Schema record1Schema = createRecord(
        createPrimitiveUnionFieldSchema("test", Schema.Type.BOOLEAN, Schema.Type.INT));
    Schema record2Schema = createRecord(
        createPrimitiveUnionFieldSchema("test", Schema.Type.INT, Schema.Type.BOOLEAN));

    GenericData.Record builderForSchema1WithBoolean = new GenericData.Record(record1Schema);
    builderForSchema1WithBoolean.put("test", true);
    GenericData.Record builderForSchema1WithInt = new GenericData.Record(record1Schema);
    builderForSchema1WithInt.put("test", 1);
    GenericData.Record builderForSchema2WithBoolean = new GenericData.Record(record2Schema);
    builderForSchema2WithBoolean.put("test", true);
    GenericData.Record builderForSchema2WithInt = new GenericData.Record(record2Schema);
    builderForSchema2WithInt.put("test", 1);

    // when

    // Evolution:
    GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithBoolean));
    GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithInt));
    GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithBoolean));
    GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithInt));

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithBoolean));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithBoolean));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithInt));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithInt));

    // then

    // Evolution:
    Assert.assertEquals(recordA.get("test"), true);
    Assert.assertEquals(recordB.get("test"), 1);
    Assert.assertEquals(recordC.get("test"), true);
    Assert.assertEquals(recordD.get("test"), 1);

    // Non-evolution
    Assert.assertEquals(recordE.get("test"), true);
    Assert.assertEquals(recordF.get("test"), true);
    Assert.assertEquals(recordG.get("test"), 1);
    Assert.assertEquals(recordH.get("test"), 1);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldTolerateUnionReorderingWithArrays(Implementation implementation) {
    // given
    Schema record1Schema = createRecord(createUnionField("test",
        Schema.create(Schema.Type.NULL),
        Schema.createArray(Schema.create(Schema.Type.INT))));
    Schema record2Schema = createRecord(createUnionField("test",
        Schema.createArray(Schema.create(Schema.Type.INT)),
        Schema.create(Schema.Type.NULL)));

    GenericData.Record builderForSchema1WithArray = new GenericData.Record(record1Schema);
    builderForSchema1WithArray.put("test", new ArrayList<Integer>());
    GenericData.Record builderForSchema1WithNull = new GenericData.Record(record1Schema);
    builderForSchema1WithNull.put("test", null);
    GenericData.Record builderForSchema2WithArray = new GenericData.Record(record2Schema);
    builderForSchema2WithArray.put("test", new ArrayList<Integer>());
    GenericData.Record builderForSchema2WithNull = new GenericData.Record(record2Schema);
    builderForSchema2WithNull.put("test", null);

    // when

    // Evolution:
    try {
      GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithArray));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithNull));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithArray));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithNull));
    } catch (Exception e) {
      // broken
    }

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithArray));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithArray));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // then

    // Evolution:
//    Assert.assertEquals((List<Integer>) recordA.get("test"), new ArrayList<Integer>());
//    Assert.assertEquals(recordB.get("test"), null);
//    Assert.assertEquals((List<Integer>) recordC.get("test"), new ArrayList<Integer>());
//    Assert.assertEquals(recordD.get("test"), null);

    // Non-evolution
    Assert.assertEquals((List<Integer>) recordE.get("test"), new ArrayList<Integer>());
    Assert.assertEquals((List<Integer>) recordF.get("test"), new ArrayList<Integer>());
    Assert.assertEquals(recordG.get("test"), null);
    Assert.assertEquals(recordH.get("test"), null);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldTolerateUnionReorderingWithMaps(Implementation implementation) {
    // given
    Schema record1Schema = createRecord(createUnionField("test",
        Schema.create(Schema.Type.NULL),
        Schema.createMap(Schema.create(Schema.Type.INT))));
    Schema record2Schema = createRecord(createUnionField("test",
        Schema.createMap(Schema.create(Schema.Type.INT)),
        Schema.create(Schema.Type.NULL)));

    GenericData.Record builderForSchema1WithArray = new GenericData.Record(record1Schema);
    builderForSchema1WithArray.put("test", new HashMap<String, Integer>());
    GenericData.Record builderForSchema1WithNull = new GenericData.Record(record1Schema);
    builderForSchema1WithNull.put("test", null);
    GenericData.Record builderForSchema2WithArray = new GenericData.Record(record2Schema);
    builderForSchema2WithArray.put("test", new HashMap<String, Integer>());
    GenericData.Record builderForSchema2WithNull = new GenericData.Record(record2Schema);
    builderForSchema2WithNull.put("test", null);

    // when

    // Evolution:
    try {
      GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithArray));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithNull));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithArray));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithNull));
    } catch (Exception e) {
      // broken
    }

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithArray));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithArray));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // then

    // Evolution:
//    Assert.assertEquals((Map<String, Integer>) recordA.get("test"), new HashMap<String, Integer>());
//    Assert.assertEquals(recordB.get("test"), null);
//    Assert.assertEquals((Map<String, Integer>) recordC.get("test"), new HashMap<String, Integer>());
//    Assert.assertEquals(recordD.get("test"), null);

    // Non-evolution
    Assert.assertEquals((Map<String, Integer>) recordE.get("test"), new HashMap<String, Integer>());
    Assert.assertEquals((Map<String, Integer>) recordF.get("test"), new HashMap<String, Integer>());
    Assert.assertEquals(recordG.get("test"), null);
    Assert.assertEquals(recordH.get("test"), null);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldTolerateUnionReorderingWithSubRecord(Implementation implementation) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createField("someInt", Schema.create(Schema.Type.INT)));

    Schema record1Schema = createRecord(createUnionField("test",
        Schema.create(Schema.Type.NULL),
        subRecordSchema));
    Schema record2Schema = createRecord(createUnionField("test",
        subRecordSchema,
        Schema.create(Schema.Type.NULL)));

    GenericData.Record subRecord = new GenericData.Record(subRecordSchema);
    subRecord.put("someInt", 1);
    GenericData.Record builderForSchema1WithSubRecord = new GenericData.Record(record1Schema);
    builderForSchema1WithSubRecord.put("test", subRecord);
    GenericData.Record builderForSchema1WithNull = new GenericData.Record(record1Schema);
    builderForSchema1WithNull.put("test", null);
    GenericData.Record builderForSchema2WithSubRecord = new GenericData.Record(record2Schema);
    builderForSchema2WithSubRecord.put("test", subRecord);
    GenericData.Record builderForSchema2WithNull = new GenericData.Record(record2Schema);
    builderForSchema2WithNull.put("test", null);

    // when

    // Evolution:
    try {
      GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithSubRecord));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithNull));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithSubRecord));
    } catch (Exception e) {
      // broken
    }
    try {
      GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithNull));
    } catch (Exception e) {
      // broken
    }

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithSubRecord));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithSubRecord));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // then

    // Evolution:
//    Assert.assertEquals(recordA.get("test"), subRecord);
//    Assert.assertEquals(recordB.get("test"), null);
//    Assert.assertEquals(recordC.get("test"), subRecord);
//    Assert.assertEquals(recordD.get("test"), null);

    // Non-evolution
    Assert.assertEquals(recordE.get("test"), subRecord);
    Assert.assertEquals(recordF.get("test"), subRecord);
    Assert.assertEquals(recordG.get("test"), null);
    Assert.assertEquals(recordH.get("test"), null);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldSkipRemovedRecord(Implementation implementation) {
    // given
    Schema subRecord1Schema = createRecord("subRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));
    Schema subRecord2Schema = createRecord("subRecord2", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));

    Schema record1Schema = createRecord(
        createField("subRecord1", subRecord1Schema),
        createField("subRecord2", subRecord2Schema),
        createUnionFieldWithNull("subRecord3", subRecord2Schema),
        createField("subRecord4", subRecord1Schema));

    Schema record2Schema = createRecord(
        createField("subRecord1", subRecord1Schema),
        createField("subRecord4", subRecord1Schema));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecord1Schema);
    subRecordBuilder.put("test1", "abc");
    subRecordBuilder.put("test2", "def");

    GenericData.Record subRecordBuilder2 = new GenericData.Record(subRecord2Schema);
    subRecordBuilder2.put("test1", "ghi");
    subRecordBuilder2.put("test2", "jkl");

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put("subRecord1", subRecordBuilder);
    builder.put("subRecord2", subRecordBuilder2);
    builder.put("subRecord3", subRecordBuilder2);
    builder.put("subRecord4", subRecordBuilder);

    // when
    GenericRecord record = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("subRecord1")).get("test1"));
    Assert.assertEquals(new Utf8("def"), ((GenericRecord) record.get("subRecord1")).get("test2"));
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("subRecord4")).get("test1"));
    Assert.assertEquals(new Utf8("def"), ((GenericRecord) record.get("subRecord4")).get("test2"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldSkipRemovedNestedRecord(Implementation implementation) {
    // given
    Schema subSubRecordSchema = createRecord("subSubRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test2", Schema.Type.STRING));
    Schema subRecord1Schema = createRecord("subRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createField("test2", subSubRecordSchema), createUnionFieldWithNull("test3", subSubRecordSchema),
        createPrimitiveFieldSchema("test4", Schema.Type.STRING));
    Schema subRecord2Schema = createRecord("subRecord", createPrimitiveFieldSchema("test1", Schema.Type.STRING),
        createPrimitiveFieldSchema("test4", Schema.Type.STRING));

    Schema record1Schema = createRecord(createField("subRecord", subRecord1Schema));

    Schema record2Schema = createRecord(createField("subRecord", subRecord2Schema));

    GenericData.Record subSubRecordBuilder = new GenericData.Record(subSubRecordSchema);
    subSubRecordBuilder.put("test1", "abc");
    subSubRecordBuilder.put("test2", "def");

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecord1Schema);
    subRecordBuilder.put("test1", "abc");
    subRecordBuilder.put("test2", subSubRecordBuilder);
    subRecordBuilder.put("test3", subSubRecordBuilder);
    subRecordBuilder.put("test4", "def");

    GenericData.Record builder = new GenericData.Record(record1Schema);
    builder.put("subRecord", subRecordBuilder);

    // when
    GenericRecord record = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) record.get("subRecord")).get("test1"));
    Assert.assertEquals(new Utf8("def"), ((GenericRecord) record.get("subRecord")).get("test4"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadMultipleChoiceUnion(Implementation implementation) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

    Schema recordSchema = createRecord(
        createUnionFieldWithNull("union", subRecordSchema, Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT)));

    GenericData.Record subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("subField", "abc");

    GenericData.Record builder = new GenericData.Record(recordSchema);
    builder.put("union", subRecordBuilder);

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericData.Record) record.get("union")).get("subField"));

    // given
    builder = new GenericData.Record(recordSchema);
    builder.put("union", "abc");

    // when
    record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(new Utf8("abc"), record.get("union"));

    // given
    builder = new GenericData.Record(recordSchema);
    builder.put("union", 1);

    // when
    record = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(builder));

    // then
    Assert.assertEquals(1, record.get("union"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadArrayOfRecords(Implementation implementation) {
    // given
    Schema recordSchema = createRecord("record", createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    Schema arrayRecordSchema = Schema.createArray(recordSchema);

    GenericData.Record subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    GenericData.Array<GenericData.Record> recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
    recordsArray.add(subRecordBuilder);
    recordsArray.add(subRecordBuilder);

    // when
    GenericData.Array<GenericRecord> array = implementation.decode(arrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(recordsArray));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals(new Utf8("abc"), array.get(0).get("field"));
    Assert.assertEquals(new Utf8("abc"), array.get(1).get("field"));

    // given

    arrayRecordSchema = Schema.createArray(createUnionSchema(recordSchema));

    subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
    recordsArray.add(subRecordBuilder);
    recordsArray.add(subRecordBuilder);

    // when
    array = implementation.decode(arrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(recordsArray));

    // then
    Assert.assertEquals(2, array.size());
    Assert.assertEquals(new Utf8("abc"), array.get(0).get("field"));
    Assert.assertEquals(new Utf8("abc"), array.get(1).get("field"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadArrayOfBoolean(Implementation implementation) {
    // given
    List<Boolean> data = new ArrayList<>(2);
    data.add(true);
    data.add(false);

    // then
    shouldReadArrayOfPrimitives(implementation, Schema.Type.BOOLEAN, PrimitiveBooleanList.class, data);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadArrayOfDouble(Implementation implementation) {
    // given
    List<Double> data = new ArrayList<>(2);
    data.add(1.0D);
    data.add(2.0D);

    // then
    shouldReadArrayOfPrimitives(implementation, Schema.Type.DOUBLE, PrimitiveDoubleList.class, data);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadArrayOfFloats(Implementation implementation) {
    // given
    List<Float> data = new ArrayList<>(2);
    data.add(1.0F);
    data.add(2.0F);

    // then
    shouldReadArrayOfPrimitives(implementation, Schema.Type.FLOAT, PrimitiveFloatList.class, data);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadArrayOfInts(Implementation implementation) {
    // given
    List<Integer> data = new ArrayList<>(2);
    data.add(1);
    data.add(2);

    // then
    shouldReadArrayOfPrimitives(implementation, Schema.Type.INT, PrimitiveIntList.class, data);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadArrayOfLongs(Implementation implementation) {
    // given
    List<Long> data = new ArrayList<>(2);
    data.add(1L);
    data.add(2L);

    // then
    shouldReadArrayOfPrimitives(implementation, Schema.Type.LONG, PrimitiveLongList.class, data);
  }

  private <E, L> void shouldReadArrayOfPrimitives(Implementation implementation, Schema.Type elementType, Class<L> expectedListClass, List<E> data) {
    // given
    Schema elementSchema = Schema.create(elementType);
    Schema arraySchema = Schema.createArray(elementSchema);

    GenericData.Array<E> avroArray = new GenericData.Array<>(0, arraySchema);
    for (E element: data) {
      avroArray.add(element);
    }

    // when
    List<E> array = implementation.decode(arraySchema, arraySchema, genericDataAsDecoder(avroArray));

    // then
    Assert.assertEquals(array.size(), data.size());
    for (int i = 0; i < data.size(); i++) {
      Assert.assertEquals(array.get(i), data.get(i));
    }

    if (implementation.isFast) {
      // The extended API should always be available, regardless of whether warm or cold
      Assert.assertTrue(Arrays.stream(array.getClass().getInterfaces()).anyMatch(c -> c.equals(expectedListClass)),
          "The returned type should implement " + expectedListClass.getSimpleName());

      try {
        Method getPrimitiveMethod = expectedListClass.getMethod("getPrimitive", int.class);
        for (int i = 0; i < data.size(); i++) {
          Assert.assertEquals(getPrimitiveMethod.invoke(array, i), data.get(i));
        }
      } catch (Exception e) {
        Assert.fail("Failed to access the getPrimitive function!");
      }
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadMapOfRecords(Implementation implementation) {
    // given
    Schema recordSchema = createRecord("record", createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

    Schema mapRecordSchema = Schema.createMap(recordSchema);

    GenericData.Record subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    Map<String, GenericData.Record> recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    recordsMap.put("2", subRecordBuilder);

    // when
    Map<String, GenericRecord> map = implementation.decode(mapRecordSchema, mapRecordSchema,
        FastSerdeTestsSupport.genericDataAsDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("1")).get("field"));
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("2")).get("field"));

    // given
    mapRecordSchema = Schema.createMap(createUnionSchema(recordSchema));

    subRecordBuilder = new GenericData.Record(recordSchema);
    subRecordBuilder.put("field", "abc");

    recordsMap = new HashMap<>();
    recordsMap.put("1", subRecordBuilder);
    recordsMap.put("2", subRecordBuilder);

    // when
    map = decodeRecordWarmFast(mapRecordSchema, mapRecordSchema,
        FastSerdeTestsSupport.genericDataAsDecoder(recordsMap, mapRecordSchema));

    // then
    Assert.assertEquals(2, map.size());
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("1")).get("field"));
    Assert.assertEquals(new Utf8("abc"), map.get(new Utf8("2")).get("field"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadNestedMap(Implementation implementation) {
    // given
    Schema nestedMapSchema = createRecord(
        createMapFieldSchema("mapField", Schema.createMap(Schema.createArray(Schema.create(Schema.Type.INT)))));

    Map<String, List> value = new HashMap<>();
    value.put("subKey1", Arrays.asList(1));
    value.put("subKey2", Arrays.asList(2));
    Map<String, Map<String, List>> mapField = new HashMap<>();
    mapField.put("key1", value);

    GenericData.Record recordData = new GenericData.Record(nestedMapSchema);
    recordData.put("mapField", mapField);

    // when
    GenericData.Record decodedRecord = implementation.decode(nestedMapSchema, nestedMapSchema,
        FastSerdeTestsSupport.genericDataAsDecoder(recordData, nestedMapSchema));

    // then
    Object decodedMapField = decodedRecord.get("mapField");
    Assert.assertEquals("{key1={subKey1=[1], subKey2=[2]}}", decodedMapField.toString());
    Assert.assertTrue(decodedMapField instanceof Map);
    Object subMap = ((Map) decodedMapField).get(new Utf8("key1"));
    Assert.assertTrue(subMap instanceof Map);
    Assert.assertEquals(Arrays.asList(1), ((List) ((Map) subMap).get(new Utf8("subKey1"))));
    Assert.assertEquals(Arrays.asList(2), ((List) ((Map) subMap).get(new Utf8("subKey2"))));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadRecursiveUnionRecord(Implementation implementation) {
    // given
    Schema unionRecordSchema = Schema.parse("{\"type\":\"record\",\"name\":\"recordName\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"fields\":[{\"name\":\"strField\",\"type\":\"string\"},{\"name\":\"unionField\",\"type\":[\"null\",\"recordName\"]}]}");

    GenericData.Record recordData = new GenericData.Record(unionRecordSchema);
    recordData.put("strField", "foo");

    GenericData.Record unionField = new GenericData.Record(unionRecordSchema);
    unionField.put("strField", "bar");
    recordData.put("unionField", unionField);

    // when
    GenericData.Record decodedRecord = implementation.decode(unionRecordSchema, unionRecordSchema,
        FastSerdeTestsSupport.genericDataAsDecoder(recordData, unionRecordSchema));

    // then
    Assert.assertEquals(new Utf8("foo"), decodedRecord.get("strField"));
    Object decodedUnionField = decodedRecord.get("unionField");
    Assert.assertTrue(decodedUnionField instanceof GenericData.Record);
    Assert.assertEquals(new Utf8("bar"), ((GenericData.Record) decodedUnionField).get("strField"));
  }

  private static <T> T decodeRecordColdFast(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastSerdeCache.FastDeserializerWithAvroGenericImpl<>(writerSchema, readerSchema);

    return decodeRecordFast(deserializer, decoder);
  }

  private static <T> T decodeRecordWarmFast(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastGenericDeserializerGenerator<T>(writerSchema, readerSchema, tempDir, classLoader,
            null).generateDeserializer();

    return decodeRecordFast(deserializer, decoder);
  }

  private static <T> T decodeRecordFast(FastDeserializer<T> deserializer, Decoder decoder) {
    try {
      return deserializer.deserialize(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T decodeRecordSlow(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    org.apache.avro.io.DatumReader<GenericData> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
    try {
      return (T) datumReader.read(null, decoder);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
