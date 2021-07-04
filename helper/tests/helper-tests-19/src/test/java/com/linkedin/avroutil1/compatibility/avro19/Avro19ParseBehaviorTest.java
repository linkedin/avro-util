/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro19ParseBehaviorTest {

  @Test
  public void demonstrateAvro19DoesntValidateRecordNamespaces() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidNamespace.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro19ValidatesRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro19CanAvoidValidatingRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro19ValidatesFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro19CanAvoidValidatingFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void demonstrateAvro19ValidatesFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro19CanAvoidValidatingFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void demonstrateAvro19ValidatesUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro19CanAvoidValidatingUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro19ValidatesFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro19CanAvoidValidatingFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro19ValidatesEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro19CanAvoidValidatingEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro19ValidatesEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro19CanAvoidValidatingEnumValues() throws Exception {
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
