package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.TestUtil;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * tests the schema parsing methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperParsingTest {

  @Test
  public void testParseSimpleSchema() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalRecord.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, null, null);
    Assert.assertNotNull(result);
    Schema mainSchema = result.getMainSchema();
    Assert.assertNotNull(mainSchema);
    Assert.assertEquals(mainSchema.getFullName(), "com.acme.PerfectlyNormalRecord");
    Map<String, Schema> allSchemas = result.getAllSchemas();
    Assert.assertEquals(allSchemas.size(), 1);
    Assert.assertEquals(mainSchema, allSchemas.get(mainSchema.getFullName()));
  }

  //TODO - add a test for validating namespaces once https://issues.apache.org/jira/browse/AVRO-2742 is settled

  @Test(expectedExceptions = SchemaParseException.class)
  public void testValidateRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(true, false);
    AvroCompatibilityHelper.parse(avsc, conf, null);
  }

  @Test
  public void testDontValidateRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, true);
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, conf, null);
    Assert.assertNotNull(result.getMainSchema());
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testValidateFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(true, false);
    AvroCompatibilityHelper.parse(avsc, conf, null);
  }

  @Test
  public void testDontValidateFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, false);
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, conf, null);
    Assert.assertNotNull(result.getMainSchema());
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void testValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, true);
    AvroCompatibilityHelper.parse(avsc, conf, null);
  }

  @Test
  public void testDontValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, false);
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, conf, null);
    Assert.assertNotNull(result.getMainSchema());
  }

  @Test(expectedExceptions = AvroTypeException.class)
  public void testValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, true);
    AvroCompatibilityHelper.parse(avsc, conf, null);
  }

  @Test
  public void testDontValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, false);
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, conf, null);
    Assert.assertNotNull(result.getMainSchema());
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testValidateFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(true, false);
    AvroCompatibilityHelper.parse(avsc, conf, null);
  }

  @Test
  public void testDontValidateFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, false);
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, conf, null);
    Assert.assertNotNull(result.getMainSchema());
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testValidateEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(true, false);
    AvroCompatibilityHelper.parse(avsc, conf, null);
  }

  @Test
  public void testDontValidateEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, false);
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, conf, null);
    Assert.assertNotNull(result.getMainSchema());
  }

  @Test(expectedExceptions = SchemaParseException.class)
  public void testValidateEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(true, false);
    AvroCompatibilityHelper.parse(avsc, conf, null);
  }

  @Test
  public void testDontValidateEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    SchemaParseConfiguration conf = new SchemaParseConfiguration(false, false);
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, conf, null);
    Assert.assertNotNull(result.getMainSchema());
  }
}
