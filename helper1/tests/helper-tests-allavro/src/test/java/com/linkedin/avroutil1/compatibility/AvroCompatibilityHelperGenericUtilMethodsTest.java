package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * tests the generic record utility methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperGenericUtilMethodsTest {

  @Test
  public void testCreateGenericEnum() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    GenericData.EnumSymbol symbol = AvroCompatibilityHelper.newEnumSymbol(schema, "A");
    Assert.assertNotNull(symbol);
    Assert.assertEquals(symbol.toString(), "A");

    symbol = AvroCompatibilityHelper.newEnumSymbol(schema, "B");
    Assert.assertNotNull(symbol);
    Assert.assertEquals(symbol.toString(), "B");
  }

  @Test
  public void testCreateGenericFixed() throws Exception {
    String avsc = TestUtil.load("PerfectlyNormalFixed.avsc");
    SchemaParseResult result = AvroCompatibilityHelper.parse(avsc, SchemaParseConfiguration.STRICT, null);
    Schema schema = result.getMainSchema();

    GenericData.Fixed fixed = AvroCompatibilityHelper.newFixedField(schema, new byte[]{1, 2, 3});
    Assert.assertNotNull(fixed);
    Assert.assertEquals(fixed.bytes(), new byte[] {1, 2, 3});

    fixed = AvroCompatibilityHelper.newFixedField(schema, null);
    Assert.assertNotNull(fixed);
    Assert.assertNull(fixed.bytes());

    fixed = AvroCompatibilityHelper.newFixedField(schema);
    Assert.assertNotNull(fixed);
    Assert.assertEquals(fixed.bytes(), new byte[] {0, 0, 0});
  }

}
