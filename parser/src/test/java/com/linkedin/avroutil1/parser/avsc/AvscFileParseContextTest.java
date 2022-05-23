package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;



public class AvscFileParseContextTest {

  @Test
  public void testResolveReferencesWithNullNamespace() throws IOException {
    String referencingAvsc = TestUtil.load("schemas/TestRecordWithInternalNullNamespaceReference.avsc");

    AvscParser parser = new AvscParser();

    AvscParseResult result1 = parser.parse(referencingAvsc);
    AvroRecordSchema schema = (AvroRecordSchema) result1.getTopLevelSchema();
    Assert.assertNotNull(schema.getField("testField").getSchema());
  }

  @Test
  public void testResolveReferencesWithNonNullNamespace() throws IOException {
    String referencingAvsc = TestUtil.load("schemas/TestRecordWithInternalNonNullNamespaceReference.avsc");

    AvscParser parser = new AvscParser();

    AvscParseResult result1 = parser.parse(referencingAvsc);
    AvroRecordSchema schema = (AvroRecordSchema) result1.getTopLevelSchema();
    Assert.assertNotNull(schema.getField("testField").getSchema());
  }
}