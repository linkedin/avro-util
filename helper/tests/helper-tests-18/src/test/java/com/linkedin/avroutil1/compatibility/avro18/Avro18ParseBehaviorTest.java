/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro18;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro18ParseBehaviorTest {

  @Test
  public void demonstrateAvro18DoesntValidateRecordNamespaces() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidNamespace.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro18ValidatesRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro18CanAvoidValidatingRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro18ValidatesFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro18CanAvoidValidatingFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro18DoesntValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void demonstrateAvro18CanValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    nativeParse(avsc, null, true);
  }

  @Test
  public void demonstrateAvro18DoesntValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void demonstrateAvro18CanValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    nativeParse(avsc, null, true);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro18ValidatesFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro18CanAvoidValidatingFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro18ValidatesEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro18CanAvoidValidatingEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro18ValidatesEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro18CanAvoidValidatingEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  private Schema nativeParse(String avsc, Boolean validateNames, Boolean validateDefaults) {
    Schema.Parser parser = new Schema.Parser();
    if (validateNames != null) {
      parser.setValidate(validateNames);
    }
    if (validateDefaults != null) {
      parser.setValidateDefaults(validateDefaults);
    }
    return parser.parse(avsc);
  }
}
