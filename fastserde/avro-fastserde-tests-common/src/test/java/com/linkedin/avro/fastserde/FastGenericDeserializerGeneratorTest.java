package com.linkedin.avro.fastserde;

import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveDoubleList;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.generated.avro.InnerRecordNotNull;
import com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNotNullComplexFields;
import com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroRecordUtil;
import com.linkedin.avroutil1.compatibility.AvroVersion;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;
import static com.linkedin.avro.fastserde.FastSpecificDeserializerGeneratorTest.createAndSerializeOuterRecordWithNotNullComplexFields;


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

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadNonUnionEnumTypesWithUnionEnumTypes(Implementation implementation) {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema writerSchema = createRecord(
            createField("testEnum", enumSchema));
    Schema readerSchema = createRecord(
            createUnionFieldWithNull("testEnum", enumSchema));

    GenericRecord originalRecord = new GenericData.Record(writerSchema);
    originalRecord.put("testEnum",
            AvroCompatibilityHelper.newEnumSymbol(enumSchema, "A"));

    // when
    GenericRecord record = implementation.decode(writerSchema, readerSchema, genericDataAsDecoder(originalRecord));

    // then
    Assert.assertEquals("A", record.get("testEnum").toString());
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

    // In order to test the functionality of the record split we set an unusually low number
    FastGenericDeserializerGenerator.setFieldsPerPopulationMethod(2);
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

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldBeAbleToReadVeryLargeSchema(Implementation implementation) {

    // given
    int n = 1000;
    Schema.Field[] fields = new Schema.Field[n];
    for(int i=0; i<n; i++) {
      fields[i] = createPrimitiveUnionFieldSchema("F" + i, Schema.Type.STRING);
    }
    Schema recordSchema = createRecord(fields);

    GenericRecord record = new GenericData.Record(recordSchema);

    for(int i=0; i<n; i++) {
      record.put("F" + i, "" + i);
    }

    // when
    GenericRecord decodedRecord = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(record));

    // then
    for(int i=0; i<n; i++) {
      Assert.assertEquals(new Utf8("" + i), decodedRecord.get("F" + i));
    }
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
    GenericData.Fixed deserializedFixed = ((GenericData.Fixed) record.get("testFixed"));
    Assert.assertEquals(new byte[]{0x01, 0x02}, deserializedFixed.bytes());

    /** Verify whether the deserialized Fixed type contains the right schema or not **/
    Method getSchemaMethod = null;
    try {
      getSchemaMethod = GenericData.Fixed.class.getMethod("getSchema");
      Assert.assertEquals(getSchemaMethod.invoke(deserializedFixed, (Object[]) null), fixedSchema);
    } catch (NoSuchMethodException e) {
      // Expected for avro-1.4
      if (!Utils.isAvro14()) {
        Assert.fail("Avro version other than 1.4 shouldn't throw NoSuchMethodException here");
      }
    } catch (Exception e) {
      Assert.fail("'getSchema' invocation shouldn't throw such exception: " + e);
    }

    Assert.assertEquals(new byte[]{0x03, 0x04}, ((GenericData.Fixed) record.get("testFixedUnion")).bytes());
    Assert.assertEquals(new byte[]{0x05, 0x06},
        ((List<GenericData.Fixed>) record.get("testFixedArray")).get(0).bytes());
    Assert.assertEquals(new byte[]{0x07, 0x08},
        ((List<GenericData.Fixed>) record.get("testFixedUnionArray")).get(0).bytes());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldHandleDiffNamepspaceInRecords(Implementation implementation) {
    //record with two fields, first optional string second a record with namespace "a.b.c" with name "innerRecordName". Inner record has one int field
    Schema writerSchema  = Schema.parse("{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"OuterRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"optionalString\",\n" +
            "      \"type\": [\"null\", \"string\"],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"innerRecord\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"innerRecordName\",\n" +
            "        \"namespace\": \"a.b.c\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"intField\",\n" +
            "            \"type\": \"int\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}");

    //change namespace on inner record to "d.e.f"
    Schema readerSchema = Schema.parse("{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"OuterRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"optionalString\",\n" +
            "      \"type\": [\"null\", \"string\"],\n" +
            "      \"default\": null\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"innerRecord\",\n" +
            "      \"type\": [\"null\", {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"innerRecordName\",\n" +
            "        \"namespace\": \"d.e.f\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"intField\",\n" +
            "            \"type\": \"int\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }],\n" +
            "      \"default\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}");

    GenericRecord originalRecord = new GenericData.Record(writerSchema);
    originalRecord.put("optionalString", "abc");
    GenericRecord innerRecord = new GenericData.Record(writerSchema.getField("innerRecord").schema());
    innerRecord.put("intField", 1);
    originalRecord.put("innerRecord", innerRecord);

    // when
    try{
      GenericRecord record = implementation.decode(writerSchema, readerSchema, genericDataAsDecoder(originalRecord));
      // then
      if(Utils.usesQualifiedNameForNamedTypedMatching()){
        Assert.fail("1.5-1.7 don't support unqualified name for named type matching so we should have failed");
      }
      Assert.assertEquals(new Utf8("abc"), record.get("optionalString"));
      GenericRecord innerRecordDecoded = (GenericRecord) record.get("innerRecord");
      Assert.assertEquals(1, innerRecordDecoded.get("intField"));
    } catch (Exception e){
      if(!Utils.usesQualifiedNameForNamedTypedMatching()) {
        Assert.fail("1.4, and 1.8+ support unqualified name for named type matching");
      }
    }
  }


  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldNotFailOnNamespaceMismatch(Implementation implementation) throws IOException {
    // writer-side schema: "metadata" has NO namespace
    String writerSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"wrapper\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"metadata\",\n" +
            "      \"type\": [\"null\", {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"metadata\",\n" +
            "        \"fields\": [\n" +
            "          {\"name\": \"fieldName\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
            "        ]\n" +
            "      }],\n" +
            "      \"default\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    // reader-side schema: same nested record but WITH namespace "rtapi.surge"
    String readerSchemaStr = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Wrapper\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"metadata\",\n" +
            "      \"type\": [\"null\", {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"metadata\",\n" +
            "        \"namespace\": \"some.other.namespace\",\n" +
            "        \"fields\": [\n" +
            "          {\"name\": \"fieldName\", \"type\": [\"null\", \"string\"], \"default\": null}\n" +
            "        ]\n" +
            "      }],\n" +
            "      \"default\": null\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    Schema writerSchema = AvroCompatibilityHelper.parse(writerSchemaStr);
    Schema readerSchema = AvroCompatibilityHelper.parse(readerSchemaStr);

    // Build a writer-side record instance
    GenericRecord wrapper = new GenericData.Record(writerSchema);
    Schema metadataSchema = writerSchema.getField("metadata").schema().getTypes().get(1);
    GenericRecord metadataRecord = new GenericData.Record(metadataSchema);
    metadataRecord.put("fieldName", "abc-123");
    wrapper.put("metadata", metadataRecord);

    // Attempt to deserialize – should throw AvroTypeException because of namespace mismatch
    try{
      GenericRecord record = implementation.decode(writerSchema, readerSchema, genericDataAsDecoder(wrapper));
      if(Utils.usesQualifiedNameForNamedTypedMatching()){
        Assert.fail("1.5-1.7 don't support unqualified name for named type matching so we should have failed");
      }
      Assert.assertEquals(((GenericRecord)record.get("metadata")).get("fieldName").toString(), "abc-123");
    } catch (AvroTypeException e){
        if(!Utils.usesQualifiedNameForNamedTypedMatching()) {
            Assert.fail("1.4, and 1.8+ support unqualified name for named type matching");
        }
        // expected exception
    }

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
  public void shouldHandleNullableCorrectly(Implementation implementation){
    //writer is a record with 2 required primitive fields, 1 required record and 1 required array field with record elements
    Schema innerWriteRecordOneSchema =  createRecord("recordOne",
            createField("fieldA", Schema.create(Schema.Type.STRING))
    );
    Schema innerWriterRecordTwoSchema =  createRecord("recordTwo",
            createField("fieldB", Schema.create(Schema.Type.STRING))
    );
    Schema writerSchema = createRecord(
        createField("field1", Schema.create(Schema.Type.STRING)),
        createField("field2", Schema.create(Schema.Type.DOUBLE)),
        createField("fieldRecordOne", innerWriteRecordOneSchema),
        createArrayFieldSchema("arrayFieldRecordTwo", innerWriterRecordTwoSchema
    ));
    //reader is a record with 4 fields, all nullable at all levels including array field
    Schema readerSchema = createRecord(
        createUnionFieldWithNull("field1", Schema.create(Schema.Type.STRING)),
        createUnionFieldWithNull("field2", Schema.create(Schema.Type.DOUBLE)),
        createUnionFieldWithNull("fieldRecordOne", createRecord("recordOne",
                createUnionFieldWithNull("fieldA", Schema.create(Schema.Type.STRING))
        )),
        createUnionFieldWithNull("arrayFieldRecordTwo", createArrayFieldSchema("unused",
                createUnionFieldWithNull("unused2",
                        createRecord("recordTwo",
                                createUnionFieldWithNull("fieldB", Schema.create(Schema.Type.STRING))
                        )
                ).schema()
        ).schema()
    ));

    //create record with writer schema
    GenericRecord originalRecord = new GenericData.Record(writerSchema);
    originalRecord.put("field1", "abc");
    originalRecord.put("field2", 1.0);
    GenericRecord innerRecord = new GenericData.Record(innerWriteRecordOneSchema);
    innerRecord.put("fieldA", "valFieldA");
    originalRecord.put("fieldRecordOne", innerRecord);
    GenericRecord innerRecord2 = new GenericData.Record(innerWriterRecordTwoSchema);
    innerRecord2.put("fieldB", "valFieldB");
    originalRecord.put("arrayFieldRecordTwo", Arrays.asList(innerRecord2));

    //decode record
    GenericRecord record = implementation.decode(writerSchema, readerSchema, genericDataAsDecoder(originalRecord));

    Assert.assertEquals(new Utf8("abc"), record.get("field1"));
    Assert.assertEquals(1.0, record.get("field2"));
    Assert.assertEquals(new Utf8("valFieldB"), ((List<GenericData.Record>) record.get("arrayFieldRecordTwo")).get(0).get("fieldB"));
    Assert.assertEquals(new Utf8("valFieldA"), ((GenericRecord) record.get("fieldRecordOne")).get("fieldA"));
  }



  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadEnumDefault(Implementation implementation) {
    if (!Utils.isAbleToSupportEnumDefault()) {
      // skip if enum default is not supported in the schema
      return;
    }

    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C"});
    Schema writerSchema = createRecord(
        createField("testEnum", enumSchema),
        createUnionFieldWithNull("testEnumUnion", enumSchema),
        createArrayFieldSchema("testEnumArray", enumSchema),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

    GenericRecord originalRecord = new GenericData.Record(writerSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "C"));
    originalRecord.put("testEnumUnion",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "C"));
    originalRecord.put("testEnumArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "C")));
    originalRecord.put("testEnumUnionArray",
        Arrays.asList(AvroCompatibilityHelper.newEnumSymbol(enumSchema, "C")));

    Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"A", "B"}, "A");
    Schema readerSchema = createRecord(
        createField("testEnum", enumSchema1),
        createUnionFieldWithNull("testEnumUnion", enumSchema1),
        createArrayFieldSchema("testEnumArray", enumSchema1),
        createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema1)));

    // when
    GenericRecord record = implementation.decode(writerSchema, readerSchema, genericDataAsDecoder(originalRecord));

    // then
    // C is missing in the schema, use default value A
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

  @Test(expectedExceptions = AvroTypeException.class, groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldDecodeRecordAndOnlyFailWhenReadingStrippedEnum(Implementation implementation) {
    // given
    Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C"});
    Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema));

    GenericRecord originalRecord = new GenericData.Record(recordSchema);
    originalRecord.put("testEnum",
        AvroCompatibilityHelper.newEnumSymbol(enumSchema, "C"));//new GenericData.EnumSymbol("C"));

    Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"A", "B"});
    Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1));

    // when
    GenericRecord record = implementation.decode(recordSchema, recordSchema1, genericDataAsDecoder(originalRecord));
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
    if (AvroCompatibilityHelper.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_9)) {
      try {
        record.get("testRemoved");
        Assert.fail("Should throw for missing fields.");
      } catch (AvroRuntimeException e) {
        // expected
      }
    } else {
      Assert.assertNull(record.get("testRemoved"));
    }
    Assert.assertEquals(new Utf8("ghi"), record.get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"), ((GenericRecord) record.get("subRecord")).get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"),
        ((List<GenericRecord>) record.get("subRecordArray")).get(0).get("testNotRemoved2"));
    Assert.assertEquals(new Utf8("ghi"),
        ((Map<String, GenericRecord>) record.get("subRecordMap")).get(new Utf8("1")).get("testNotRemoved2"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType(Implementation implementation) {
    // given
    Schema record1Schema = createRecord(
        createPrimitiveUnionFieldSchema("test", Schema.Type.STRING, Schema.Type.LONG));
    Schema record2Schema = createRecord(
        createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));

    GenericData.Record builderForSchema1WithString = new GenericData.Record(record1Schema);
    builderForSchema1WithString.put("test", "abc");
    GenericData.Record builderForSchema1WithLong = new GenericData.Record(record1Schema);
    builderForSchema1WithLong.put("test", 1L);
    GenericData.Record builderForSchema2WithString = new GenericData.Record(record2Schema);
    builderForSchema2WithString.put("test", "abc");

    // when

    // Evolution:
    GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithString));
    GenericRecord recordB = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithString));

    // Non-evolution
    GenericRecord recordC = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithString));
    GenericRecord recordD = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithString));
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithLong));

    // Broken
    try {
      GenericRecord recordF = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithLong));
      Assert.fail("Should have thrown an AvroTypeException.");
    } catch (AvroTypeException e) {
      // Expected
    }

    // then

    // Evolution:
    Assert.assertEquals(recordA.get("test"), new Utf8("abc"));
    Assert.assertEquals(recordB.get("test"), new Utf8("abc"));

    // Non-evolution
    Assert.assertEquals(recordC.get("test"), new Utf8("abc"));
    Assert.assertEquals(recordD.get("test"), new Utf8("abc"));
    Assert.assertEquals(recordE.get("test"), 1L);
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
    GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithInt));
    GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithString));
    GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithInt));

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithString));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithString));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithInt));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithInt));

    // then

    // Evolution:
    Assert.assertEquals(recordA.get("test"), new Utf8("abc"));
    Assert.assertEquals(recordB.get("test"), 1);
    Assert.assertEquals(recordC.get("test"), new Utf8("abc"));
    Assert.assertEquals(recordD.get("test"), 1);

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
    GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithArray));
    GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithArray));
    GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithArray));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithArray));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // then

    // Evolution:
    Assert.assertEquals((List<Integer>) recordA.get("test"), new ArrayList<Integer>());
    Assert.assertEquals(recordB.get("test"), null);
    Assert.assertEquals((List<Integer>) recordC.get("test"), new ArrayList<Integer>());
    Assert.assertEquals(recordD.get("test"), null);

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
    GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithArray));
    GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithArray));
    GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithArray));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithArray));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // then

    // Evolution:
    Assert.assertEquals((Map<String, Integer>) recordA.get("test"), new HashMap<String, Integer>());
    Assert.assertEquals(recordB.get("test"), null);
    Assert.assertEquals((Map<String, Integer>) recordC.get("test"), new HashMap<String, Integer>());
    Assert.assertEquals(recordD.get("test"), null);

    // Non-evolution
    Assert.assertEquals((Map<String, Integer>) recordE.get("test"), new HashMap<String, Integer>());
    Assert.assertEquals((Map<String, Integer>) recordF.get("test"), new HashMap<String, Integer>());
    Assert.assertEquals(recordG.get("test"), null);
    Assert.assertEquals(recordH.get("test"), null);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldTolerateUnionReorderingWithSubRecords(Implementation implementation) {
    // given
    Schema subRecordSchema1 = createRecord("subRecord1", createField("someInt1", Schema.create(Schema.Type.INT)));
    Schema subRecordSchema2 = createRecord("subRecord2", createField("someInt2", Schema.create(Schema.Type.INT)));

    Schema record1Schema = createRecord(createUnionField("test",
        Schema.create(Schema.Type.NULL),
        subRecordSchema1,
        subRecordSchema2));
    Schema record2Schema = createRecord(createUnionField("test",
        subRecordSchema2,
        subRecordSchema1,
        Schema.create(Schema.Type.NULL)));

    GenericData.Record subRecord = new GenericData.Record(subRecordSchema1);
    subRecord.put("someInt1", 1);
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
    GenericRecord recordA = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithSubRecord));
    GenericRecord recordB = implementation.decode(record1Schema, record2Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordC = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithSubRecord));
    GenericRecord recordD = implementation.decode(record2Schema, record1Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // Non-evolution
    GenericRecord recordE = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithSubRecord));
    GenericRecord recordF = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithSubRecord));
    GenericRecord recordG = implementation.decode(record1Schema, record1Schema, genericDataAsDecoder(builderForSchema1WithNull));
    GenericRecord recordH = implementation.decode(record2Schema, record2Schema, genericDataAsDecoder(builderForSchema2WithNull));

    // then

    // Evolution:
    Assert.assertEquals(recordA.get("test"), subRecord);
    Assert.assertEquals(recordB.get("test"), null);
    Assert.assertEquals(recordC.get("test"), subRecord);
    Assert.assertEquals(recordD.get("test"), null);

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

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReturnMutableEmptyMap(Implementation implementation) {
    Schema recordSchema = createRecord("TestRecord", createMapFieldSchema("map_field", Schema.create(Schema.Type.STRING)));
    GenericRecord recordBuilder = new GenericData.Record(recordSchema);
    recordBuilder.put("map_field", Collections.emptyMap());

    GenericRecord decodedRecord = implementation.decode(recordSchema, recordSchema, genericDataAsDecoder(recordBuilder));
    Object mapField = decodedRecord.get("map_field");
    Assert.assertTrue(mapField instanceof HashMap && ((HashMap)mapField).isEmpty(),
        "The decoded empty map should be an instance of HashMap, but got: " + mapField.getClass());
  }

  // The test case in which one record is split into many with the usage of aliases. Example: record A with aliases B and C in the next version is changed into records B and C, both of them have an alias to A.
  // This test contains such a migration in both ways.
  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadSplittedAndAliasedSubRecordFields(Implementation implementation) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createPrimitiveUnionFieldSchema("intField", Schema.Type.INT), createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    subRecordSchema = FastSerdeTestsSupport.addAliases(subRecordSchema, "com.linkedin.avro.fastserde.generated.avro.aliasedSubRecord");

    Schema recordSchema = createRecord(
        createField("record1", subRecordSchema),
        createField("record2", subRecordSchema),
        createArrayFieldSchema("recordArray", subRecordSchema));

    Schema aliasedSubRecordSchema = createRecord("aliasedSubRecord", createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
    aliasedSubRecordSchema = FastSerdeTestsSupport.addAliases(aliasedSubRecordSchema, "com.linkedin.avro.fastserde.generated.avro.subRecord");

    Schema splittedAndAliasedRecordSchema = createRecord(
        createField("record1", aliasedSubRecordSchema),
        createField("record2", subRecordSchema),
        createArrayFieldSchema("recordArray", aliasedSubRecordSchema));

    GenericRecord subRecordBuilder = new GenericData.Record(subRecordSchema);
    subRecordBuilder.put("intField", 1);
    subRecordBuilder.put("subField", "abc");

    GenericRecord aliasedSubRecordBuilder = new GenericData.Record(aliasedSubRecordSchema);
    aliasedSubRecordBuilder.put("subField", "abc");

    GenericRecord forwardRecordBuilder = new GenericData.Record(splittedAndAliasedRecordSchema);
    forwardRecordBuilder.put("record1", aliasedSubRecordBuilder);
    forwardRecordBuilder.put("record2", subRecordBuilder);
    forwardRecordBuilder.put("recordArray", Collections.singletonList(aliasedSubRecordBuilder));

    // when
    GenericRecord forwardRecord = implementation.decode(splittedAndAliasedRecordSchema, recordSchema, genericDataAsDecoder(forwardRecordBuilder));

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) forwardRecord.get("record1")).get("subField"));
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) forwardRecord.get("record1")).getSchema().hashCode());
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) forwardRecord.get("record2")).get("subField"));
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) forwardRecord.get("record2")).getSchema().hashCode());

    GenericRecord backwardRecordBuilder = new GenericData.Record(recordSchema);
    backwardRecordBuilder.put("record1", subRecordBuilder);
    backwardRecordBuilder.put("record2", subRecordBuilder);
    backwardRecordBuilder.put("recordArray", Collections.singletonList(subRecordBuilder));

    // when
    GenericRecord backwardRecord = implementation.decode(recordSchema, splittedAndAliasedRecordSchema, genericDataAsDecoder(backwardRecordBuilder));

    // then
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) backwardRecord.get("record1")).get("subField"));
    Assert.assertEquals(aliasedSubRecordSchema.hashCode(), ((GenericRecord) backwardRecord.get("record1")).getSchema().hashCode());
    Assert.assertEquals(new Utf8("abc"), ((GenericRecord) backwardRecord.get("record2")).get("subField"));
    Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) backwardRecord.get("record2")).getSchema().hashCode());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldFailLikeVanillaAvroWhenReadingUnionTypeWithIncompatibleNonUnionType(Implementation implementation) {
    // given
    Schema unionRecordSchema = createRecord("record", createUnionField("someField", Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING)));
    Schema recordSchema = createRecord("record", createField("someField", Schema.create(Schema.Type.INT)));

    GenericData.Record record = new GenericData.Record(unionRecordSchema);
    record.put("someField", "string");

    // when & then
    Assert.assertThrows(AvroTypeException.class, () -> implementation.decode(unionRecordSchema, recordSchema, genericDataAsDecoder(record)));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadTopLevelNonUnionTypesWithUnionTypes(Implementation implementation) {
    // given
    Schema recordSchema = createRecord("record", createField("someInt", Schema.create(Schema.Type.INT)));
    Schema unionRecordSchema = createUnionSchema(recordSchema);


    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("someInt", 1);

    // when
    GenericRecord recordA = implementation.decode(recordSchema, unionRecordSchema, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(recordA.get("someInt"), 1);


    // given
    Schema arrayRecordSchema = Schema.createArray(recordSchema);
    Schema unionArrayRecordSchema = createUnionSchema(arrayRecordSchema);

    GenericData.Array<GenericData.Record> array = new GenericData.Array<>(2, arrayRecordSchema);
    array.add(record);
    array.add(record);

    // when
    GenericData.Array<GenericData.Record> arrayA = implementation.decode(arrayRecordSchema, unionArrayRecordSchema, genericDataAsDecoder(array));

    // then
    Assert.assertEquals(arrayA.size(), 2);
    Assert.assertEquals(arrayA.get(0).get("someInt"), 1);
    Assert.assertEquals(arrayA.get(1).get("someInt"), 1);

    // given
    Schema mapRecordSchema = Schema.createMap(recordSchema);
    Schema unionMapRecordSchema = createUnionSchema(mapRecordSchema);

    Map<String, GenericData.Record> map = new HashMap<>();
    map.put("one", record);
    map.put("two", record);

    // when
    Map<Utf8, GenericData.Record> mapA = implementation.decode(mapRecordSchema, unionMapRecordSchema, genericDataAsDecoder(map, mapRecordSchema));

    // then
    Assert.assertEquals(mapA.size(), 2);
    Assert.assertEquals(mapA.get(new Utf8("one")).get("someInt"), 1);
    Assert.assertEquals(mapA.get(new Utf8("two")).get("someInt"), 1);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadTopLevelUnionTypesWithCompatibleNonUnionTypes(Implementation implementation) {
    // given
    Schema recordSchema = createRecord("record", createField("someInt", Schema.create(Schema.Type.INT)));
    Schema unionRecordSchema = createUnionSchema(recordSchema);


    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("someInt", 1);

    // when
    GenericRecord recordA = implementation.decode(unionRecordSchema, recordSchema, genericDataAsDecoder(record, unionRecordSchema));

    // then
    Assert.assertEquals(recordA.get("someInt"), 1);


    // given
    Schema arrayRecordSchema = Schema.createArray(recordSchema);
    Schema unionArrayRecordSchema = createUnionSchema(arrayRecordSchema);

    GenericData.Array<GenericData.Record> array = new GenericData.Array<>(2, arrayRecordSchema);
    array.add(record);
    array.add(record);

    // when
    GenericData.Array<GenericData.Record> arrayA = implementation.decode(unionArrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(array, unionArrayRecordSchema));

    // then
    Assert.assertEquals(arrayA.size(), 2);
    Assert.assertEquals(arrayA.get(0).get("someInt"), 1);
    Assert.assertEquals(arrayA.get(1).get("someInt"), 1);

    // given
    Schema mapRecordSchema = Schema.createMap(recordSchema);
    Schema unionMapRecordSchema = createUnionSchema(mapRecordSchema);

    Map<String, GenericData.Record> map = new HashMap<>();
    map.put("one", record);
    map.put("two", record);

    // when
    Map<Utf8, GenericData.Record> mapA = implementation.decode(unionMapRecordSchema, mapRecordSchema, genericDataAsDecoder(map, unionMapRecordSchema));

    // then
    Assert.assertEquals(mapA.size(), 2);
    Assert.assertEquals(mapA.get(new Utf8("one")).get("someInt"), 1);
    Assert.assertEquals(mapA.get(new Utf8("two")).get("someInt"), 1);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldBidirectionallyReadPrimitiveWithUnionPrimitive(Implementation implementation) {
    // given
    Schema recordSchema1 = createRecord("record", createField("someInt", Schema.create(Schema.Type.INT)));
    Schema recordSchema2 = createRecord("record", createUnionField("someInt", Schema.create(Schema.Type.INT)));

    GenericData.Record record = new GenericData.Record(recordSchema1);
    record.put("someInt", 1);

    // when
    GenericRecord recordA = implementation.decode(recordSchema1, recordSchema2, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(recordA.get("someInt"), 1);

    // given
    record = new GenericData.Record(recordSchema2);
    record.put("someInt", 1);

    // when
    GenericRecord recordB = implementation.decode(recordSchema2, recordSchema1, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(recordB.get("someInt"), 1);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldBidirectionallyReadRecordWithUnionRecord(Implementation implementation) {
    // given
    Schema subRecordSchema = createRecord("subRecord", createField("someInt1", Schema.create(Schema.Type.INT)),
            createField("someInt2", Schema.create(Schema.Type.INT)));

    Schema recordSchema = createRecord("record", createField("subRecord", subRecordSchema));
    Schema recordSchemaWithUnion = createRecord("record", createField("subRecord", createUnionSchema(subRecordSchema)));

    GenericData.Record subRecord = new GenericData.Record(subRecordSchema);
    subRecord.put("someInt1", 1);
    subRecord.put("someInt2", 2);

    GenericData.Record record = new GenericData.Record(recordSchema);
    record.put("subRecord", subRecord);

    // when
    GenericRecord recordA = implementation.decode(recordSchema, recordSchemaWithUnion, genericDataAsDecoder(record));

    // then
    Assert.assertNotNull(recordA.get("subRecord"));
    Assert.assertEquals(((GenericData.Record) recordA.get("subRecord")).get("someInt1"), 1);
    Assert.assertEquals(((GenericData.Record) recordA.get("subRecord")).get("someInt2"), 2);

    // given
    record = new GenericData.Record(recordSchemaWithUnion);
    record.put("subRecord", subRecord);

    // when
    GenericRecord recordB = implementation.decode(recordSchemaWithUnion, recordSchema, genericDataAsDecoder(record));

    // then
    Assert.assertNotNull(recordB.get("subRecord"));
    Assert.assertEquals(((GenericData.Record) recordB.get("subRecord")).get("someInt1"), 1);
    Assert.assertEquals(((GenericData.Record) recordB.get("subRecord")).get("someInt2"), 2);
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldBidirectionallyReadArrayWithUnionArray(Implementation implementation) {
    // given
    Schema recordWithArraySchema = createRecord("record", createArrayFieldSchema("someInts", Schema.create(Schema.Type.INT)));
    Schema recordWithUnionArraySchema = createRecord("record", createUnionField("someInts", Schema.create(Schema.Type.NULL), Schema.createArray(Schema.create(Schema.Type.INT))));

    List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5);
    GenericData.Record record = new GenericData.Record(recordWithArraySchema);
    record.put("someInts", expected);

    // when
    GenericRecord recordA = implementation.decode(recordWithArraySchema, recordWithUnionArraySchema, genericDataAsDecoder(record));

    // then
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(((List<Integer>) recordA.get("someInts")).get(i), expected.get(i));
    }

    record = new GenericData.Record(recordWithUnionArraySchema);
    record.put("someInts", expected);

    // when
    GenericRecord recordB = implementation.decode(recordWithUnionArraySchema, recordWithArraySchema, genericDataAsDecoder(record));

    // then
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(((List<Integer>) recordB.get("someInts")).get(i), expected.get(i));
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadMapWithNestedRecordWithMapField(Implementation implementation) {
    // given
    Schema schema = createRecord("outerRecord",
            createField("mapField", createMapFieldSchema("mapField",
                    createRecord("innerRecord", createField("internalMapField",
                            createMapFieldSchema("internalMapField", Schema.create(Schema.Type.LONG)).schema())
                    )).schema()));
    Schema mapSchema = schema.getField("mapField").schema();
    Schema recordFieldSchema = mapSchema.getValueType();

    // Create test data
    GenericRecord record = new GenericData.Record(schema);
    Map<String, GenericRecord> mapField = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      GenericRecord nestedRecord = new GenericData.Record(recordFieldSchema);
      Map<String, Long> internalMap = new HashMap<>();
      internalMap.put("key" + i, (long) i);
      nestedRecord.put("internalMapField", internalMap);
      mapField.put("record" + i, nestedRecord);
    }
    record.put("mapField", mapField);

    // when
    GenericRecord decoded = implementation.decode(schema, schema, genericDataAsDecoder(record));
    //then
    Assert.assertNotNull(decoded);
    Map<Utf8, GenericRecord> deserializedMap = (Map<Utf8, GenericRecord>) decoded.get("mapField");
    Assert.assertEquals(deserializedMap.size(), 3);
    for (int i = 0; i < 3; i++) {
      GenericRecord nestedRecord = deserializedMap.get(new Utf8("record" + i));
      Assert.assertNotNull(nestedRecord);
      Map<Utf8, Long> internalMap = (Map<Utf8, Long>) nestedRecord.get("internalMapField");
      Assert.assertEquals(internalMap.get(new Utf8("key" + i)), Long.valueOf(i));
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldReadNestedArrayOfMaps(Implementation implementation) {
    // Define a complex nested structure: array<array<map<string, int>>>
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
    Schema innerArraySchema = Schema.createArray(mapSchema);
    Schema outerArraySchema = Schema.createArray(innerArraySchema);
    Schema recordSchema = createRecord(
            createField("arrayField", outerArraySchema)
    );

    GenericRecord recordData = new GenericData.Record(recordSchema);
    List<List<Map<String, Integer>>> outerArray = new ArrayList<>();
    List<Map<String, Integer>> innerArray1 = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      Map<String, Integer> map = new HashMap<>();
      map.put("key" + i + "_1", i * 10 + 1);
      map.put("key" + i + "_2", i * 10 + 2);
      map.put("key" + i + "_3", i * 10 + 3);
      innerArray1.add(map);
    }

    List<Map<String, Integer>> innerArray2 = new ArrayList<>();
    Map<String, Integer> map = new HashMap<>();
    map.put("keyA", 100);
    map.put("keyB", 200);
    map.put("keyC", 300);
    innerArray2.add(map);

    outerArray.add(innerArray1);
    outerArray.add(innerArray2);
    recordData.put("arrayField", outerArray);

    // when
    GenericData.Record decodedRecord = implementation.decode(recordSchema, recordSchema,
            FastSerdeTestsSupport.genericDataAsDecoder(recordData, recordSchema));

    // then
    Object decodedArrayField = decodedRecord.get("arrayField");
    Assert.assertTrue(decodedArrayField instanceof List);
    List<?> decodedOuterArray = (List<?>) decodedArrayField;
    Assert.assertEquals(decodedOuterArray.size(), 2);

    // Verify first inner array with 2 maps
    Object firstInnerArray = decodedOuterArray.get(0);
    Assert.assertTrue(firstInnerArray instanceof List);
    List<?> decodedInnerArray1 = (List<?>) firstInnerArray;
    Assert.assertEquals(decodedInnerArray1.size(), 2);

    // Verify the maps in the first inner array
    for (int i = 0; i < 2; i++) {
      Object mapObj = decodedInnerArray1.get(i);
      Assert.assertTrue(mapObj instanceof Map);
      @SuppressWarnings("unchecked")
      Map<Utf8, Integer> decodedMap = (Map<Utf8, Integer>) mapObj;
      Assert.assertEquals(decodedMap.size(), 3);

      // Verify each key-value pair in the map
      Assert.assertEquals(decodedMap.get(new Utf8("key" + i + "_1")), Integer.valueOf(i * 10 + 1));
      Assert.assertEquals(decodedMap.get(new Utf8("key" + i + "_2")), Integer.valueOf(i * 10 + 2));
      Assert.assertEquals(decodedMap.get(new Utf8("key" + i + "_3")), Integer.valueOf(i * 10 + 3));
    }

    // Verify second inner array with 1 map
    Object secondInnerArray = decodedOuterArray.get(1);
    Assert.assertTrue(secondInnerArray instanceof List);
    List<?> decodedInnerArray2 = (List<?>) secondInnerArray;
    Assert.assertEquals(decodedInnerArray2.size(), 1);

    // Verify the map in the second inner array
    Map<Utf8, Integer> decodedMap = (Map<Utf8, Integer>) decodedInnerArray2.get(0);
    Assert.assertEquals(decodedMap.size(), 3);
    Assert.assertEquals(decodedMap.get(new Utf8("keyA")), Integer.valueOf(100));
    Assert.assertEquals(decodedMap.get(new Utf8("keyB")), Integer.valueOf(200));
    Assert.assertEquals(decodedMap.get(new Utf8("keyC")), Integer.valueOf(300));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldBidirectionallyReadArrayWithUnionArrayOfUnionValues(Implementation implementation) {
    // given
    Schema recordWithArraySchema = createRecord("record", createArrayFieldSchema("someInts", Schema.create(Schema.Type.INT)));
    Schema recordWithUnionArrayOfUnionValuesSchema = createRecord("record", createUnionField("someInts", Schema.create(Schema.Type.NULL),
            Schema.createArray(Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL))))));

    List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5);
    GenericData.Record record = new GenericData.Record(recordWithArraySchema);
    record.put("someInts", expected);

    // when
    GenericRecord recordA = implementation.decode(recordWithArraySchema, recordWithUnionArrayOfUnionValuesSchema, genericDataAsDecoder(record));

    // then

    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(((List<Integer>) recordA.get("someInts")).get(i), expected.get(i));
    }

    // given
    record = new GenericData.Record(recordWithUnionArrayOfUnionValuesSchema);
    record.put("someInts", expected);

    // when
    GenericRecord recordB = implementation.decode(recordWithUnionArrayOfUnionValuesSchema, recordWithArraySchema, genericDataAsDecoder(record));

    // then
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(((List<Integer>) recordB.get("someInts")).get(i), expected.get(i));
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldBidirectionallyReadMapWithUnionMap(Implementation implementation) {
    // given
    Schema recordWithMapSchema = createRecord("record", createMapFieldSchema("someInts", Schema.create(Schema.Type.INT)));
    Schema recordWithUnionMapSchema = createRecord("record", createUnionField("someInts", Schema.create(Schema.Type.NULL), Schema.createMap(Schema.create(Schema.Type.INT))));

    Map<String, Integer> map = new HashMap<>();
    map.put("1", 1);
    map.put("2", 2);
    map.put("3", 3);
    GenericData.Record record = new GenericData.Record(recordWithMapSchema);
    record.put("someInts", map);

    // when
    GenericRecord recordA = implementation.decode(recordWithMapSchema, recordWithUnionMapSchema, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(((Map<Utf8,Integer>) recordA.get("someInts")).get(new Utf8("1")), Integer.valueOf(1));
    Assert.assertEquals(((Map<Utf8,Integer>) recordA.get("someInts")).get(new Utf8("2")), Integer.valueOf(2));
    Assert.assertEquals(((Map<Utf8,Integer>) recordA.get("someInts")).get(new Utf8("3")), Integer.valueOf(3));

    // given
    record = new GenericData.Record(recordWithUnionMapSchema);
    record.put("someInts", map);

    // when
    GenericRecord recordB = implementation.decode(recordWithUnionMapSchema, recordWithMapSchema, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(((Map<Utf8,Integer>) recordB.get("someInts")).get(new Utf8("1")), Integer.valueOf(1));
    Assert.assertEquals(((Map<Utf8,Integer>) recordB.get("someInts")).get(new Utf8("2")), Integer.valueOf(2));
    Assert.assertEquals(((Map<Utf8,Integer>) recordB.get("someInts")).get(new Utf8("3")), Integer.valueOf(3));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldHandleJavaKeywordsAsRecordNames(Implementation implementation) {
    // Create schemas with Java keywords as names
    Schema classSchema = createRecord("class",
            createField("value", Schema.create(Schema.Type.STRING)));

    Schema ifSchema = createRecord("if",
            createField("value", Schema.create(Schema.Type.STRING)));

    Schema publicSchema = createRecord("public",
            createField("value", Schema.create(Schema.Type.STRING)));

    // Create a parent record that contains records with Java keyword names
    Schema parentSchema = createRecord("ParentRecord",
            createField("class_field", classSchema),
            createField("if_field", ifSchema),
            createField("public_field", publicSchema));

    // Create record instances
    GenericRecord classRecord = new GenericData.Record(classSchema);
    classRecord.put("value", "class value");

    GenericRecord ifRecord = new GenericData.Record(ifSchema);
    ifRecord.put("value", "if value");

    GenericRecord publicRecord = new GenericData.Record(publicSchema);
    publicRecord.put("value", "public value");

    GenericRecord parentRecord = new GenericData.Record(parentSchema);
    parentRecord.put("class_field", classRecord);
    parentRecord.put("if_field", ifRecord);
    parentRecord.put("public_field", publicRecord);


    // when
    GenericRecord deserializedRecord = implementation.decode(parentSchema, parentSchema,
            genericDataAsDecoder(parentRecord));

    // then
    Assert.assertEquals(((GenericRecord)deserializedRecord.get("class_field")).get("value"), new Utf8("class value"));
    Assert.assertEquals(((GenericRecord)deserializedRecord.get("if_field")).get("value"), new Utf8("if value"));
    Assert.assertEquals(((GenericRecord)deserializedRecord.get("public_field")).get("value"), new Utf8("public value"));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void shouldBidirectionallyReadMapWithUnionMapOfUnionValues(Implementation implementation) {
    // given
    Schema recordWithMapSchema = createRecord("record", createMapFieldSchema("someInts", Schema.create(Schema.Type.INT)));
    Schema recordWithUnionMapOfUnionValuesSchema = createRecord("record", createUnionField("someInts", Schema.create(Schema.Type.NULL),
            Schema.createMap(Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))))));

    Map<String, Integer> map = new HashMap<>();
    map.put("1", 1);
    map.put("2", 2);
    map.put("3", 3);
    GenericData.Record record = new GenericData.Record(recordWithMapSchema);
    record.put("someInts", map);

    // when
    GenericRecord recordA = implementation.decode(recordWithMapSchema, recordWithUnionMapOfUnionValuesSchema, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(((Map<Utf8,Integer>) recordA.get("someInts")).get(new Utf8("1")), Integer.valueOf(1));
    Assert.assertEquals(((Map<Utf8,Integer>) recordA.get("someInts")).get(new Utf8("2")), Integer.valueOf(2));
    Assert.assertEquals(((Map<Utf8,Integer>) recordA.get("someInts")).get(new Utf8("3")), Integer.valueOf(3));

    record = new GenericData.Record(recordWithUnionMapOfUnionValuesSchema);
    record.put("someInts", map);

    // when
    GenericRecord recordB = implementation.decode(recordWithUnionMapOfUnionValuesSchema, recordWithMapSchema, genericDataAsDecoder(record));

    // then
    Assert.assertEquals(((Map<Utf8,Integer>) recordB.get("someInts")).get(new Utf8("1")), Integer.valueOf(1));
    Assert.assertEquals(((Map<Utf8,Integer>) recordB.get("someInts")).get(new Utf8("2")), Integer.valueOf(2));
    Assert.assertEquals(((Map<Utf8,Integer>) recordB.get("someInts")).get(new Utf8("3")), Integer.valueOf(3));
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  void deserializeNullableFieldsPreviouslySerializedAsNotNull(Implementation implementation) throws IOException {
    // given: outerRecord1 serialized using schema with not-null complex fields
    Pair<OuterRecordWithNestedNotNullComplexFields, byte[]> pair = createAndSerializeOuterRecordWithNotNullComplexFields();
    OuterRecordWithNestedNotNullComplexFields outerRecord1 = pair.first();
    byte[] serializedOuterRecord1 = pair.second();

    Schema writerSchema = OuterRecordWithNestedNotNullComplexFields.SCHEMA$; // without nullable fields
    Schema readerSchema = OuterRecordWithNestedNullableComplexFields.SCHEMA$; // contains nullable fields
    BinaryDecoder binaryDecoder = AvroCompatibilityHelper.newBinaryDecoder(serializedOuterRecord1);

    // when: serialized outerRecord1 is deserialized using readerSchema with nullable complex fields
    GenericRecord outerRecord2 = implementation.decode(writerSchema, readerSchema, binaryDecoder);

    // then: deserialized outerRecord2 is the same as outerRecord1 (initial one)
    Assert.assertNotNull(outerRecord2);
    Assert.assertEquals(outerRecord2.toString(), outerRecord1.toString());
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  void testNestedMapWithNullableInnerMapOnReader(Implementation implementation) {

    String writerAvroSchema = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"container\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"nested_maps\",\n" +
            "    \"type\" : {\n" +
            "      \"type\" : \"map\",\n" +
            "      \"values\" : {\n" +
            "        \"type\" : \"map\",\n" +
            "        \"values\" : \"int\"\n" +
            "      }\n" +
            "    }\n" +
            "  } ]\n" +
            "}";
    Schema writerSchema = Schema.parse(writerAvroSchema);
    String readerAvroSchema = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"container\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"nested_maps\",\n" +
            "    \"type\" : [ \"null\", {\n" +
            "      \"type\" : \"map\",\n" +
            "      \"values\" : [ \"null\", {\n" +
            "        \"type\" : \"map\",\n" +
            "        \"values\" : [ \"null\", \"int\" ]\n" +
            "      } ]\n" +
            "    } ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";
    Schema readerSchema = Schema.parse(readerAvroSchema);
    
    GenericRecord record = new GenericData.Record(writerSchema);
    Map<String, Map<Utf8, Integer>> map = new HashMap<>();
    Map<Utf8, Integer> innerMap = new HashMap<>();
    innerMap.put(new Utf8("key1"), 1);
    map.put("key1", innerMap);
    
    record.put("nested_maps", map);

    // when
    GenericRecord decodedRecord = implementation.decode(writerSchema, readerSchema, genericDataAsDecoder(record));
    
    // then
    Assert.assertNotNull(decodedRecord);
    Assert.assertNotNull(decodedRecord.get("nested_maps"));
    
    @SuppressWarnings("unchecked")
    Map<Utf8, Map<Utf8, Integer>> decodedMap = (Map<Utf8, Map<Utf8, Integer>>) decodedRecord.get("nested_maps");
    Assert.assertEquals(decodedMap.size(), 1);
    
    Map<Utf8, Integer> decodedInnerMap = decodedMap.get(new Utf8("key1"));
    Assert.assertNotNull(decodedInnerMap);
    Assert.assertEquals(decodedInnerMap.size(), 1);
    Assert.assertEquals(decodedInnerMap.get(new Utf8("key1")), Integer.valueOf(1));
  }


  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  public void testNestedArrayWithNullableInnerArrayOnReader(Implementation implementation){
    String writerSchemaStr = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"message\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"data\",\n" +
            "    \"type\" : {\n" +
            "      \"type\" : \"array\",\n" +
            "      \"items\" : {\n" +
            "        \"type\" : \"array\",\n" +
            "        \"items\" : \"int\"\n" +
            "      }\n" +
            "    }\n" +
            "  } ]\n" +
            "}";
    Schema writerSchema = Schema.parse(writerSchemaStr);
    
    String readerSchemaStr = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"message\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"data\",\n" +
            "    \"type\" : [ \"null\", {\n" +
            "      \"type\" : \"array\",\n" +
            "      \"items\" : {\n" +
            "        \"type\" : \"array\",\n" +
            "        \"items\" : \"int\"\n" +
            "      }\n" +
            "    } ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";
    Schema readerSchema = Schema.parse(readerSchemaStr);
    
    // Create a test record with nested arrays
    GenericRecord record = new GenericData.Record(writerSchema);
    
    // Create a nested array structure: array<array<int>>
    List<List<Integer>> nestedArrays = new ArrayList<>();
    
    List<Integer> inner1 = new ArrayList<>();
    inner1.add(1);
    
    // Add inner arrays to the outer array
    nestedArrays.add(inner1);
    
    // Add the nested array structure to the record
    record.put("data", nestedArrays);

    // when
    GenericRecord decodedRecord = implementation.decode(writerSchema, readerSchema, genericDataAsDecoder(record));
    
    // then
    Assert.assertNotNull(decodedRecord);
    Assert.assertNotNull(decodedRecord.get("data"));
    
    @SuppressWarnings("unchecked")
    List<List<Integer>> decodedData = (List<List<Integer>>) decodedRecord.get("data");
    Assert.assertEquals(decodedData.size(), 1);

    List<Integer> decodedInner1 = decodedData.get(0);
    Assert.assertEquals(decodedInner1.get(0), Integer.valueOf(1));
    
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "Implementation")
  void testNullableNestedMap(Implementation implementation) {
    String schemaStr = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"message\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"data\",\n" +
            "    \"type\" : [ \"null\", {\n" +
            "      \"type\" : \"map\",\n" +
            "      \"values\" : {\n" +
            "        \"type\" : \"map\",\n" +
            "        \"values\" : \"int\"\n" +
            "      }\n" +
            "    } ],\n" +
            "    \"default\" : null\n" +
            "  } ]\n" +
            "}";
    Schema schema = Schema.parse(schemaStr);
    
    // Test Case 1: With non-null data
    GenericRecord record1 = new GenericData.Record(schema);
    
    // Create a nested map structure: map<string, map<string, int>>
    Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
    Map<String, Integer> innerMap = new HashMap<>();
    innerMap.put("inner_key", 42);
    nestedMap.put("outer_key", innerMap);
    
    // Add the nested map to the record
    record1.put("data", nestedMap);

    // When: decode with identical schema
    GenericRecord decodedRecord1 = implementation.decode(schema, schema, genericDataAsDecoder(record1));
    
    // Then: verify the decoded record
    Assert.assertNotNull(decodedRecord1);
    Assert.assertNotNull(decodedRecord1.get("data"));
    
    @SuppressWarnings("unchecked")
    Map<Utf8, Map<Utf8, Integer>> decodedMap = (Map<Utf8, Map<Utf8, Integer>>) decodedRecord1.get("data");
    Assert.assertEquals(decodedMap.size(), 1);
    
    Map<Utf8, Integer> decodedInnerMap = decodedMap.get(new Utf8("outer_key"));
    Assert.assertNotNull(decodedInnerMap);
    Assert.assertEquals(decodedInnerMap.size(), 1);
    Assert.assertEquals(decodedInnerMap.get(new Utf8("inner_key")), Integer.valueOf(42));
  }

  private static <T> T decodeRecordColdFast(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastSerdeUtils.FastDeserializerWithAvroGenericImpl<>(writerSchema, readerSchema, GenericData.get(), false);

    return decodeRecordFast(deserializer, decoder);
  }

  private static <T> T decodeRecordWarmFast(Schema writerSchema, Schema readerSchema, Decoder decoder) {
    FastDeserializer<T> deserializer =
        new FastGenericDeserializerGenerator<T>(writerSchema, readerSchema, tempDir, classLoader,
            null, null).generateDeserializer();

    return decodeRecordFast(deserializer, decoder);
  }

  private static <T> T decodeRecordFast(FastDeserializer<T> deserializer, Decoder decoder) {
    try {
      return deserializer.deserialize(null, decoder);
    } catch (AvroTypeException e) {
      throw e;
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
