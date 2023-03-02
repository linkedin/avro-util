/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


// Tests version detection against Avro 1.11.0
public class AvroCompatibilityHelperAvro1110Test {

  @Test
  public void testAvroVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_11;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testAvroCompilerVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_11;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroCompilerVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testAddNamespaceFillInnerNamespace() throws Exception {
    String nameSpace = "TestDatabase";

    String originalAvsc = TestUtil.load("CCDV.avsc");
    Schema originalSchema = Schema.parse(originalAvsc);

    //original schema does not have namespace
    Assert.assertNull(originalSchema.getNamespace());
    //inner schema does not have namespace
    Assert.assertNull(originalSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace());

    //build new schema and add namespace at top level
    Schema newSchema = AvroCompatibilityHelper.newSchema(originalSchema).setNamespace(nameSpace).build();

    //new schema has namespace
    Assert.assertEquals(newSchema.getNamespace(), nameSpace);
    //inner schema also has same namespace
    Assert.assertEquals(newSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace(),
        nameSpace);

    String newAvsc = newSchema.toString(true);
    String expectedAvsc = TestUtil.load("CCDV-namespace.avsc");
    Assert.assertEquals(newAvsc, expectedAvsc);
  }

  @Test
  public void testAddNamespaceNotResetInnerNamespace() throws Exception {
    String nameSpace = "TestDatabase";
    String originalInnerNamespace = "OriginalSpace";

    String originalAvsc = TestUtil.load("CCDV2.avsc");
    Schema originalSchema = Schema.parse(originalAvsc);

    //original schema does not have namespace
    Assert.assertNull(originalSchema.getNamespace());
    //inner schema has namespace
    Assert.assertEquals(originalSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace(),
        originalInnerNamespace);

    //build new schema and add namespace at top level
    Schema newSchema = AvroCompatibilityHelper.newSchema(originalSchema).setNamespace(nameSpace).build();

    //new schema has namespace
    Assert.assertEquals(newSchema.getNamespace(), nameSpace);
    //inner schema's namespace does not change
    Assert.assertEquals(newSchema.getField("records").schema().getTypes().get(1).getElementType().getNamespace(),
        originalInnerNamespace);

    String newAvsc = newSchema.toString(true);
    String expectedAvsc = TestUtil.load("CCDV2-namespace.avsc");
    Assert.assertEquals(newAvsc, expectedAvsc);
  }
}
