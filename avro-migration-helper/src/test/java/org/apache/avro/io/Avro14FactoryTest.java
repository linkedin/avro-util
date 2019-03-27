/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import com.linkedin.avro.TestUtil;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import net.openhft.compiler.CompilerUtils;
import org.apache.avro.Schema;
import org.apache.avro.io.Avro14Factory;
import org.apache.avro.specific.SpecificFixed;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.io.Avro14Factory.*;


public class Avro14FactoryTest {

  private final static String FIXED_TYPE_SCHEMA_JSON =
            "{\n"
          + "  \"type\" : \"fixed\",\n"
          + "  \"name\" : \"Whatever\",\n"
          + "  \"namespace\" : \"com.acme\",\n"
          + "  \"size\" : 42,\n"
          + "  \"doc\" : \"yadda yadda evil quote\\\"\"\n"
          + "}";
  private final static String FIXED_TYPE_NO_NAMESPACE_SCHEMA_JSON =
      "{\n"
          + "  \"type\" : \"fixed\",\n"
          + "  \"name\" : \"Whatever\",\n"
          + "  \"size\" : 42,\n"
          + "  \"doc\" : \"w00t\"\n"
          + "}";
  private final static String ENUM_CLASS_JSON =
      "{\n"
          + "  \"type\":\"enum\",\n"
          + "  \"name\":\"BobSmith\",\n"
          + "  \"namespace\":\"com.dot\",\n"
          + "  \"symbols\":[\"Bread\",\"Butter\",\"Jam\"],\n"
          + "  \"doc\" : \"Bob Smith Store\"\n"
          + "  }";
  private final static String ENUM_CLASS_NO_NAMESPACE_JSON =
      "{\n"
          + "  \"type\":\"enum\",\n"
          + "  \"name\":\"BobSmith\",\n"
          + "  \"symbols\":[\"Bread\",\"Butter\",\"Jam\"],\n"
          + "  \"doc\" : \"Bob Smith Store\"\n"
          + "  }";

  private Avro14Factory _factory;

  @BeforeClass
  public void skipForModernAvro() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    List<AvroVersion> supportedVersions = Collections.singletonList(AvroVersion.AVRO_1_4);
    if (!supportedVersions.contains(runtimeVersion)) {
      throw new SkipException("class only supported under " + supportedVersions + ". runtime version detected as " + runtimeVersion);
    }
    _factory = new Avro14Factory();
  }

  @Test
  public void testAddSchemaSupportToEnum() throws Exception {
    Schema parsed = AvroCompatibilityHelper.parse(ENUM_CLASS_JSON);
    Collection<AvroGeneratedSourceCode> compiled = _factory.compile(Collections.singletonList(parsed), AvroVersion.AVRO_1_4);
    Assert.assertEquals(1, compiled.size());
    AvroGeneratedSourceCode sourceCode = compiled.iterator().next();

    Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava("com.dot.BobSmith", sourceCode.getContents());
    Assert.assertNotNull(aClass);
    Field schemaField = aClass.getField("SCHEMA$");
    Assert.assertNotNull(schemaField);
    Schema schema = (Schema) schemaField.get(null);
    Assert.assertNotNull(schema);
    Assert.assertEquals("BobSmith", schema.getName());
    Assert.assertEquals("com.dot", schema.getNamespace());
    Assert.assertTrue(schema.getDoc().contains("Bob Smith Store"));
  }

  @Test
  public void testAddSchemaSupportToEnumNoNamespace() throws Exception {
    Schema parsed = AvroCompatibilityHelper.parse(ENUM_CLASS_NO_NAMESPACE_JSON);
    Collection<AvroGeneratedSourceCode> compiled = _factory.compile(Collections.singletonList(parsed), AvroVersion.AVRO_1_4);
    Assert.assertEquals(1, compiled.size());
    AvroGeneratedSourceCode sourceCode = compiled.iterator().next();

    Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava("BobSmith", sourceCode.getContents());
    Assert.assertNotNull(aClass);
    Field schemaField = aClass.getField("SCHEMA$");
    Assert.assertNotNull(schemaField);
    Schema schema = (Schema) schemaField.get(null);
    Assert.assertNotNull(schema);
    Assert.assertEquals("BobSmith", schema.getName());
    Assert.assertNull(schema.getNamespace());
    Assert.assertTrue(schema.getDoc().contains("Bob Smith Store"));
  }

  @Test
  public void testAddSchemaSupportToFixedClass() throws Exception {
    Schema parsed = AvroCompatibilityHelper.parse(FIXED_TYPE_SCHEMA_JSON);
    Collection<AvroGeneratedSourceCode> compiled = _factory.compile(Collections.singletonList(parsed), AvroVersion.AVRO_1_4);
    Assert.assertEquals(1, compiled.size());
    AvroGeneratedSourceCode sourceCode = compiled.iterator().next();

    Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava("com.acme.Whatever", sourceCode.getContents());
    Assert.assertNotNull(aClass);
    Assert.assertTrue(SpecificFixed.class.isAssignableFrom(aClass));
    Field schemaField = aClass.getField("SCHEMA$");
    Assert.assertNotNull(schemaField);
    Schema schema = (Schema) schemaField.get(null);
    Assert.assertNotNull(schema);
    Assert.assertEquals("Whatever", schema.getName());
    Assert.assertEquals("com.acme", schema.getNamespace());
    Assert.assertEquals(42, schema.getFixedSize());
    Assert.assertTrue(schema.getDoc().contains("yadda yadda"));
  }

  @Test
  public void testAddSchemaSupportToFixedClassNoNamespace() throws Exception {
    Schema parsed = AvroCompatibilityHelper.parse(FIXED_TYPE_NO_NAMESPACE_SCHEMA_JSON);
    Collection<AvroGeneratedSourceCode> compiled = _factory.compile(Collections.singletonList(parsed), AvroVersion.AVRO_1_4);
    Assert.assertEquals(1, compiled.size());
    AvroGeneratedSourceCode sourceCode = compiled.iterator().next();

    Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava("Whatever", sourceCode.getContents());
    Assert.assertNotNull(aClass);
    Assert.assertTrue(SpecificFixed.class.isAssignableFrom(aClass));
    Field schemaField = aClass.getField("SCHEMA$");
    Assert.assertNotNull(schemaField);
    Schema schema = (Schema) schemaField.get(null);
    Assert.assertNotNull(schema);
    Assert.assertEquals("Whatever", schema.getName());
    Assert.assertNull(schema.getNamespace());
    Assert.assertEquals(42, schema.getFixedSize());
    Assert.assertTrue(schema.getDoc().contains("w00t"));
  }

  @Test
  public void testSafeSplit() throws Exception {
    Assert.assertEquals(
        Arrays.asList("1234567890", "abcdefghij"),
        safeSplit("1234567890abcdefghij", 10));
    Assert.assertEquals(
        Arrays.asList("1234567890", "abcdefghij", "AB"),
        safeSplit("1234567890abcdefghijAB", 10));
    Assert.assertEquals(Collections.singletonList("1234567890"),
        safeSplit("1234567890", 10));
    //dont chop at '
    Assert.assertEquals(
        Arrays.asList("12345678", "9'abcdefgh", "ij"),
        safeSplit("123456789'abcdefghij", 10));
    //unicode escapes not on the boundary
    Assert.assertEquals(
        Arrays.asList("xx\\u1234xx", "xxxxxxxxxx"),
        safeSplit("xx\\u1234xxxxxxxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxx\\u1234", "xxxxxxxxxx"),
        safeSplit("xxxx\\u1234xxxxxxxxxx", 10));
    //unicode escapes cross the boundary
    Assert.assertEquals(
        Arrays.asList("xxxx","x\\u1234xxx", "xxxxxx"),
        safeSplit("xxxxx\\u1234xxxxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxx","x\\u1234xxx", "xxxxx"),
        safeSplit("xxxxxx\\u1234xxxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxx","x\\u1234xxx", "xxxx"),
        safeSplit("xxxxxxx\\u1234xxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxx","x\\u1234xxx", "xxx"),
        safeSplit("xxxxxxxx\\u1234xxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxx","x\\u1234xxx", "xx"),
        safeSplit("xxxxxxxxx\\u1234xxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
        safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
        safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxxxx","x\\u1234xxx"),
        safeSplit("xxxxxxxxxxx\\u1234xxx", 10));
  }

  @Test
  public void testSchemaCanonicalization() throws Exception {
    Schema withDocs = Schema.parse(TestUtil.load("HasSymbolDocs.avsc"));
    Schema withoutDocs = Schema.parse(TestUtil.load("HasNoSymbolDocs.avsc"));
    Assert.assertNotEquals(withDocs.toString(true), withoutDocs.toString(true));
    String c1 = _factory.toParsingForm(withDocs);
    String c2 = _factory.toParsingForm(withoutDocs);
    Assert.assertEquals(c1, c2);
  }
}
