/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro16ParseBehaviorTest {

  @Test
  public void demonstrateAvro16DoesntValidateRecordNamespaces() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidNamespace.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro16ValidatesRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro16CanAvoidValidatingRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro16ValidatesFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro16CanAvoidValidatingFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro16DoesntValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro16DoesntValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro16ValidatesFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro16CanAvoidValidatingFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro16ValidatesEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro16CanAvoidValidatingEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro16ValidatesEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro16CanAvoidValidatingEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  private Schema nativeParse(String avsc, Boolean validateNames) {
    Schema.Parser parser = new Schema.Parser();
    if (validateNames != null) {
      parser.setValidate(validateNames);
    }
    return parser.parse(avsc);
  }
}
