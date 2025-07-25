package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.AnotherSubRecord;
import com.linkedin.avro.fastserde.generated.avro.StringableRecord;
import com.linkedin.avro.fastserde.generated.avro.StringableSubRecord;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.*;


public class FastStringableTest {
  private static final Schema javaStringSchema = Schema.parse("{\n" + "  \"type\":\"string\",\n" + "  \"avro.java.string\":\"String\"} ");
  private static final Schema javaStringKeyedMapOfJavaStringsSchema = Schema.parse("{"+
          "\"type\":\"map\",\"values\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"avro.java.string\":\"String\"}");

  private File tempDir;
  private ClassLoader classLoader;

  public FastStringableTest() {
  }

  @DataProvider(name = "SlowFastDeserializer")
  public static Object[][] deserializers() {
    return new Object[][]{{true}, {false}};
  }

  @BeforeTest(groups = {"deserializationTest", "serializationTest"})
  public void prepare() throws Exception {
    tempDir = getCodeGenDirectory();
    classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
        FastDeserializerDefaultsTest.class.getClassLoader());

    // In order to test the functionality of the record split we set an unusually low number
    FastGenericSerializerGenerator.setFieldsPerRecordSerializationMethod(2);
  }

  StringableRecord generateRecord(URL exampleURL, URI exampleURI, File exampleFile, BigDecimal exampleBigDecimal,
      BigInteger exampleBigInteger, String exampleString) {

    StringableSubRecord subRecord = new StringableSubRecord();
    if (Utils.isAbleToSupportStringableProps()) {
      subRecord.put(0, exampleURI);
    } else {
      subRecord.put(0, exampleURI.toString());
    }
    AnotherSubRecord anotherSubRecord = new AnotherSubRecord();
    anotherSubRecord.put(0, subRecord);

    StringableRecord record = new StringableRecord();
    if (Utils.isAbleToSupportStringableProps()) {
      record.put(0, exampleBigInteger);
      record.put(1, exampleBigDecimal);
      record.put(2, exampleURI);
      record.put(3, exampleURL);
      record.put(4, exampleFile);
      record.put(5, Collections.singletonList(exampleURL));
      record.put(6, Collections.singletonMap(exampleURL, exampleBigInteger));
      record.put(7, subRecord);
      record.put(8, anotherSubRecord);
      record.put(9, exampleString);
    } else {
      // Avro-1.4 doesn't support stringable field
      record.put(0, exampleBigInteger.toString());
      record.put(1, exampleBigDecimal.toString());
      record.put(2, exampleURI.toString());
      record.put(3, exampleURL.toString());
      record.put(4, exampleFile.toString());
      record.put(5, Collections.singletonList(exampleURL.toString()));
      record.put(6, Collections.singletonMap(exampleURL.toString(), exampleBigInteger.toString()));
      record.put(7, subRecord);
      record.put(8, anotherSubRecord);
      record.put(9, exampleString);
    }
    return record;
  }

  @Test(groups = {"serializationTest"})
  public void serializeStringableFields() throws URISyntaxException, MalformedURLException {
    // given
    BigInteger exampleBigInteger = new BigInteger(String.valueOf(Long.MAX_VALUE)).pow(16);
    BigDecimal exampleBigDecimal = new BigDecimal(Double.MIN_VALUE).pow(16);
    File exampleFile = new File("/tmp/test");
    URI exampleURI = new URI("urn:ISSN:1522-3611");
    URL exampleURL = new URL("http://www.asdaldjaldladjal.sadjad");
    String exampleString = "test_string";

    if (Utils.isSupportedAvroVersionsForSerializer()) {
      StringableRecord record =
          generateRecord(exampleURL, exampleURI, exampleFile, exampleBigDecimal, exampleBigInteger, exampleString);

      // when
      StringableRecord afterDecoding =
          specificDataFromDecoder(StringableRecord.SCHEMA$, writeWithFastAvro(record, StringableRecord.SCHEMA$, true));

      // then
      if (Utils.isAbleToSupportStringableProps()) {
        Assert.assertEquals(exampleBigDecimal, getField(afterDecoding, "bigdecimal"));
        Assert.assertEquals(exampleBigInteger, getField(afterDecoding, "biginteger"));
        Assert.assertEquals(exampleFile, getField(afterDecoding, "file"));
        Assert.assertEquals(Collections.singletonList(exampleURL), getField(afterDecoding, "urlArray"));
        Assert.assertEquals(Collections.singletonMap(exampleURL, exampleBigInteger), getField(afterDecoding, "urlMap"));
        Assert.assertNotNull(getField(afterDecoding, "subRecord"));
        Assert.assertEquals(exampleURI, getField((StringableSubRecord) getField(afterDecoding, "subRecord"), "uriField"));
        Assert.assertNotNull(getField(afterDecoding, "subRecordWithSubRecord"));
        Assert.assertNotNull(getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"), "subRecord"));
        Assert.assertEquals(exampleURI, getField((StringableSubRecord) getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"), "subRecord"), "uriField"));
      } else {
        Assert.assertEquals(exampleBigDecimal.toString(), getField(afterDecoding, "bigdecimal").toString());
        Assert.assertEquals(exampleBigInteger.toString(), getField(afterDecoding, "biginteger").toString());
        Assert.assertEquals(exampleFile.toString(), getField(afterDecoding, "file").toString());
        Assert.assertEquals(Collections.singletonList(new Utf8(exampleURL.toString())), (List) getField(afterDecoding, "urlArray"));
        Assert.assertEquals(
            Collections.singletonMap(new Utf8(exampleURL.toString()), new Utf8(exampleBigInteger.toString())),
            getField(afterDecoding, "urlMap"));
        Assert.assertNotNull(getField(afterDecoding, "subRecord"));
        Assert.assertEquals(exampleURI.toString(), getField((StringableSubRecord) getField(afterDecoding, "subRecord"), "uriField").toString());
        Assert.assertNotNull(getField(afterDecoding, "subRecordWithSubRecord"));
        Assert.assertNotNull(getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"), "subRecord"));
        Assert.assertEquals(exampleURI.toString(), getField((StringableSubRecord) getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"),"subRecord"), "uriField").toString());
      }
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  @SuppressWarnings("unchecked")
  public void javaStringPropertyTest(Boolean whetherUseFastDeserializer) {
    Schema schema = createRecord(
            createField("testString", javaStringSchema),
            createUnionFieldWithNull("testUnionString", javaStringSchema),
            createArrayFieldSchema("testStringArray", javaStringSchema),
            createField("testStringMap", javaStringKeyedMapOfJavaStringsSchema)
    );

    GenericRecord record = new GenericData.Record(schema);
    record.put("testString", "aaa");
    record.put("testUnionString", "bbb");
    GenericData.Array<String> stringArray = new GenericData.Array<>(0, Schema.createArray(javaStringSchema));
    stringArray.add("ccc");
    record.put("testStringArray", stringArray);
    Map<String, String> stringMap = new HashMap<>();
    stringMap.put("ddd", "eee");
    record.put("testStringMap", stringMap);

    Decoder decoder = writeWithFastAvro(record, schema, false);

    GenericRecord afterDecoding;
    if (whetherUseFastDeserializer) {
      afterDecoding = readWithFastAvro(schema, schema, decoder, false);
    } else {
      afterDecoding = readWithSlowAvro(schema, schema, decoder, false);
    }

    if (Utils.isAbleToSupportJavaStrings()){
      Assert.assertTrue(afterDecoding.get(0) instanceof String, "String is expected, but got: " + afterDecoding.get(0).getClass());
      Assert.assertTrue(afterDecoding.get(1) instanceof String, "String is expected, but got: " + afterDecoding.get(0).getClass());
      Assert.assertTrue(((GenericData.Array<Object>) afterDecoding.get(2)).get(0) instanceof String,
          "String is expected, but got: " + ((GenericData.Array<Object>) afterDecoding.get(2)).get(0).getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecoding.get(3)).keySet().iterator().next() instanceof String,
          "String is expected, but got: " + ((Map<Object, Object>) afterDecoding.get(3)).keySet().iterator().next().getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecoding.get(3)).values().iterator().next() instanceof String,
          "String is expected, but got: " + ((Map<Object, Object>) afterDecoding.get(3)).values().iterator().next().getClass());
    } else {
      Assert.assertTrue(afterDecoding.get(0) instanceof Utf8, "Utf8 is expected, but got: " + afterDecoding.get(0).getClass());
      Assert.assertTrue(afterDecoding.get(1) instanceof Utf8, "Utf8 is expected, but got: " + afterDecoding.get(0).getClass());
      Assert.assertTrue(((GenericData.Array<Object>) afterDecoding.get(2)).get(0) instanceof Utf8,
          "Utf8 is expected, but got: " + ((GenericData.Array<Object>) afterDecoding.get(2)).get(0).getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecoding.get(3)).keySet().iterator().next() instanceof Utf8,
          "Utf8 is expected, but got: " + ((Map<Object, Object>) afterDecoding.get(3)).keySet().iterator().next().getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecoding.get(3)).values().iterator().next() instanceof Utf8,
          "Utf8 is expected, but got: " + ((Map<Object, Object>) afterDecoding.get(3)).values().iterator().next().getClass());
    }
  }

  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  @SuppressWarnings("unchecked")
  public void javaStringPropertyInReaderSchemaTest(Boolean whetherUseFastDeserializer) {
    Schema writerSchema = createRecord(
            createField("testString", Schema.create(Schema.Type.STRING)),
            createUnionFieldWithNull("testUnionString", Schema.create(Schema.Type.STRING)),
            createArrayFieldSchema("testStringArray", Schema.create(Schema.Type.STRING)),
            createMapFieldSchema("testStringMap", Schema.create(Schema.Type.STRING))
    );
    Schema readerSchema = createRecord(
            createField("testString", javaStringSchema),
            createUnionFieldWithNull("testUnionString", javaStringSchema),
            createArrayFieldSchema("testStringArray", javaStringSchema),
            createField("testStringMap", javaStringKeyedMapOfJavaStringsSchema)
    );

    GenericRecord record = new GenericData.Record(writerSchema);
    record.put("testString", "aaa");
    record.put("testUnionString", "bbb");
    GenericData.Array<String> stringArray = new GenericData.Array<>(0, Schema.createArray(Schema.create(Schema.Type.STRING)));
    stringArray.add("ccc");
    record.put("testStringArray", stringArray);
    Map<String, String> stringMap = new HashMap<>();
    stringMap.put("ddd", "eee");
    record.put("testStringMap", stringMap);

    Decoder decoder1 = writeWithFastAvro(record, writerSchema, false);
    Decoder decoder2 = writeWithFastAvro(record, writerSchema, false);

    GenericRecord afterDecodingWithJavaString, afterDecodingWithoutJavaString;
    if (whetherUseFastDeserializer) {
      afterDecodingWithJavaString = readWithFastAvro(writerSchema, readerSchema, decoder1, false);
      afterDecodingWithoutJavaString = readWithFastAvro(writerSchema, writerSchema, decoder2, false);
    } else {
      afterDecodingWithJavaString = readWithSlowAvro(writerSchema, readerSchema, decoder1, false);
      afterDecodingWithoutJavaString = readWithSlowAvro(writerSchema, writerSchema, decoder2, false);
    }

    if (Utils.isAbleToSupportJavaStrings()){
      Assert.assertTrue(afterDecodingWithJavaString.get(0) instanceof String, "String is expected, but got: " + afterDecodingWithJavaString.get(0).getClass());
      Assert.assertTrue(afterDecodingWithJavaString.get(1) instanceof String, "String is expected, but got: " + afterDecodingWithJavaString.get(0).getClass());
      Assert.assertTrue(((GenericData.Array<Object>) afterDecodingWithJavaString.get(2)).get(0) instanceof String,
          "String is expected, but got: " + ((GenericData.Array<Object>) afterDecodingWithJavaString.get(2)).get(0).getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecodingWithJavaString.get(3)).keySet().iterator().next() instanceof String,
          "String is expected, but got: " + ((Map<Object, Object>) afterDecodingWithJavaString.get(3)).keySet().iterator().next().getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecodingWithJavaString.get(3)).values().iterator().next() instanceof String,
          "String is expected, but got: " + ((Map<Object, Object>) afterDecodingWithJavaString.get(3)).values().iterator().next().getClass());
    }  else {
      Assert.assertTrue(afterDecodingWithJavaString.get(0) instanceof Utf8, "Utf8 is expected, but got: " + afterDecodingWithJavaString.get(0).getClass());
      Assert.assertTrue(afterDecodingWithJavaString.get(1) instanceof Utf8, "Utf8 is expected, but got: " + afterDecodingWithJavaString.get(0).getClass());
      Assert.assertTrue(((GenericData.Array<Object>) afterDecodingWithJavaString.get(2)).get(0) instanceof Utf8,
          "Utf8 is expected, but got: " + ((GenericData.Array<Object>) afterDecodingWithJavaString.get(2)).get(0).getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecodingWithJavaString.get(3)).keySet().iterator().next() instanceof Utf8,
          "Utf8 is expected, but got: " + ((Map<Object, Object>) afterDecodingWithJavaString.get(3)).keySet().iterator().next().getClass());
      Assert.assertTrue(((Map<Object, Object>) afterDecodingWithJavaString.get(3)).values().iterator().next() instanceof Utf8,
          "Utf8 is expected, but got: " + ((Map<Object, Object>) afterDecodingWithJavaString.get(3)).values().iterator().next().getClass());
    }

    // Regardless of the above, we should also be able to decode to Utf8 within the same JVM that already decoded Java Strings
    Assert.assertTrue(afterDecodingWithoutJavaString.get(0) instanceof Utf8, "Utf8 is expected, but got: " + afterDecodingWithoutJavaString.get(0).getClass());
    Assert.assertTrue(afterDecodingWithoutJavaString.get(1) instanceof Utf8, "Utf8 is expected, but got: " + afterDecodingWithoutJavaString.get(0).getClass());
    Assert.assertTrue(((GenericData.Array<Object>) afterDecodingWithoutJavaString.get(2)).get(0) instanceof Utf8,
        "Utf8 is expected, but got: " + ((GenericData.Array<Object>) afterDecodingWithoutJavaString.get(2)).get(0).getClass());
    Assert.assertTrue(((Map<Object, Object>) afterDecodingWithoutJavaString.get(3)).keySet().iterator().next() instanceof Utf8,
        "Utf8 is expected, but got: " + ((Map<Object, Object>) afterDecodingWithoutJavaString.get(3)).keySet().iterator().next().getClass());
    Assert.assertTrue(((Map<Object, Object>) afterDecodingWithoutJavaString.get(3)).values().iterator().next() instanceof Utf8,
        "Utf8 is expected, but got: " + ((Map<Object, Object>) afterDecodingWithoutJavaString.get(3)).values().iterator().next().getClass());

  }


  @Test(groups = {"deserializationTest"}, dataProvider = "SlowFastDeserializer")
  public void deserializeStringableFields(Boolean whetherUseFastDeserializer)
      throws URISyntaxException, MalformedURLException {
    // given
    BigInteger exampleBigInteger = new BigInteger(String.valueOf(Long.MAX_VALUE)).pow(16);
    BigDecimal exampleBigDecimal = new BigDecimal(Double.MIN_VALUE).pow(16);
    File exampleFile = new File("/tmp/test");
    URI exampleURI = new URI("urn:ISSN:1522-3611");
    URL exampleURL = new URL("http://www.asdaldjaldladjal.sadjad");
    String exampleString = "test_string";

    StringableRecord record = generateRecord(exampleURL, exampleURI, exampleFile, exampleBigDecimal, exampleBigInteger, exampleString);

    // when
    StringableRecord afterDecoding = null;
    if (whetherUseFastDeserializer) {
      afterDecoding =
          readWithFastAvro(StringableRecord.SCHEMA$, StringableRecord.SCHEMA$, writeWithFastAvro(record, StringableRecord.SCHEMA$, true), true);
    } else {
      afterDecoding =
          readWithSlowAvro(StringableRecord.SCHEMA$, StringableRecord.SCHEMA$, specificDataAsDecoder(record), true);
    }

    // then
    if (Utils.isAbleToSupportStringableProps()) {
      Assert.assertEquals(exampleBigDecimal, getField(afterDecoding, "bigdecimal"));
      Assert.assertEquals(exampleBigInteger, getField(afterDecoding, "biginteger"));
      Assert.assertEquals(exampleFile, getField(afterDecoding, "file"));
      Assert.assertEquals(Collections.singletonList(exampleURL), getField(afterDecoding, "urlArray"));
      Assert.assertEquals(Collections.singletonMap(exampleURL, exampleBigInteger), getField(afterDecoding, "urlMap"));
      Assert.assertNotNull(getField(afterDecoding, "subRecord"));
      Assert.assertEquals(exampleURI, getField((StringableSubRecord) getField(afterDecoding, "subRecord"), "uriField"));
      Assert.assertNotNull(getField(afterDecoding, "subRecordWithSubRecord"));
      Assert.assertNotNull(getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"), "subRecord"));
      Assert.assertEquals(exampleURI, getField((StringableSubRecord) getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"), "subRecord"), "uriField"));
    } else {
      Assert.assertEquals(exampleBigDecimal.toString(), getField(afterDecoding, "bigdecimal").toString());
      Assert.assertEquals(exampleBigInteger.toString(), getField(afterDecoding, "biginteger").toString());
      Assert.assertEquals(exampleFile.toString(), getField(afterDecoding, "file").toString());
      Assert.assertEquals(Collections.singletonList(new Utf8(exampleURL.toString())), (List) getField(afterDecoding, "urlArray"));
      Assert.assertEquals(
          Collections.singletonMap(new Utf8(exampleURL.toString()), new Utf8(exampleBigInteger.toString())),
          getField(afterDecoding, "urlMap"));
      Assert.assertNotNull(getField(afterDecoding, "subRecord"));
      Assert.assertEquals(exampleURI.toString(), getField(((StringableSubRecord) getField(afterDecoding, "subRecord")), "uriField").toString());
      Assert.assertNotNull(getField(afterDecoding, "subRecordWithSubRecord"));
      Assert.assertNotNull(getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"), "subRecord"));
      Assert.assertEquals(exampleURI.toString(), getField(((StringableSubRecord) getField((AnotherSubRecord) getField(afterDecoding, "subRecordWithSubRecord"), "subRecord")), "uriField").toString());
    }
  }

  public <T> Decoder writeWithFastAvro(T data, Schema schema, boolean specific) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null);

    try {
      FastSerializer<T> fastSerializer;
      if (specific) {
        FastSpecificSerializerGenerator<T> fastSpecificSerializerGenerator = new FastSpecificSerializerGenerator<>(schema, tempDir, classLoader, null, null);
        fastSerializer = fastSpecificSerializerGenerator.generateSerializer();
      } else {
        FastGenericSerializerGenerator<T> fastGenericSerializerGenerator = new FastGenericSerializerGenerator<>(schema, tempDir, classLoader, null, null);
        fastSerializer = fastGenericSerializerGenerator.generateSerializer();
      }
      fastSerializer.serialize(data, binaryEncoder);
      binaryEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray(), null);
  }

  public <T> T readWithFastAvro(Schema writerSchema, Schema readerSchema, Decoder decoder, boolean specific) {
    FastDeserializer<T> deserializer;
    if (specific) {
      deserializer = new FastSpecificDeserializerGenerator<T>(writerSchema, readerSchema, tempDir, classLoader, null, null).generateDeserializer();
    } else {
      deserializer = new FastGenericDeserializerGenerator<T>(writerSchema, readerSchema, tempDir, classLoader, null, null).generateDeserializer();
    }
    try {
      return deserializer.deserialize(decoder);
    } catch (AvroTypeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T readWithSlowAvro(Schema writerSchema, Schema readerSchema, Decoder decoder, boolean specific) {
    DatumReader<T> datumReader;
    if (specific) {
      datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
    } else {
      datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
    }
    try {
      return datumReader.read(null, decoder);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
