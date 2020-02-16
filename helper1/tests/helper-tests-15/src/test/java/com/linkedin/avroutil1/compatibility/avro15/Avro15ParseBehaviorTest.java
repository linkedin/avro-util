package com.linkedin.avroutil1.compatibility.avro15;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro15ParseBehaviorTest {

  @Test
  public void demonstrateAvro14DoesntValidateRecordNamespaces() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidNamespace.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro15ValidatesRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro15CanAvoidValidatingRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro15ValidatesFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro15CanAvoidValidatingFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro15DoesntValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro15DoesntValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    Schema parsedByVanilla = nativeParse(avsc, null);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro15ValidatesFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro15CanAvoidValidatingFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro15ValidatesEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro15CanAvoidValidatingEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    Schema parsedByVanilla = nativeParse(avsc, false);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateAvro15ValidatesEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    nativeParse(avsc, null);
  }

  @Test
  public void demonstrateAvro15CanAvoidValidatingEnumValues() throws Exception {
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
