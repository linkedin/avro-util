package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro17ParseBehaviorTest {

  @Test
  public void demonstrateAvro17DoesntValidateRecordNamespaces() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidNamespace.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro17ValidatesRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro17CanAvoidValidatingRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro17ValidatesFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro17CanAvoidValidatingFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro17DoesntValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void demonstrateAvro17CanValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    nativeParse(avsc, null, true);
  }

  @Test
  public void demonstrateAvro17DoesntValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void demonstrateAvro17CanValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    nativeParse(avsc, null, true);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro17ValidatesFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro17CanAvoidValidatingFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro17ValidatesEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro17CanAvoidValidatingEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro17ValidatesEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    nativeParse(avsc, null, null);
  }

  @Test
  public void demonstrateAvro17CanAvoidValidatingEnumValues() throws Exception {
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
