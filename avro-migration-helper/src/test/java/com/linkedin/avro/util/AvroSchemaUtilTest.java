/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.util;

import com.linkedin.avro.TestUtil;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroSchemaUtilTest {

  @Test
  public void testFindNonNullUnionBranch() throws Exception {
    Schema withUnion = AvroCompatibilityHelper.parse(TestUtil.load("HasUnions.avsc"));
    Schema fieldSchema = AvroSchemaUtil.findNonNullUnionBranch(withUnion, "f1");
    Assert.assertNotNull(fieldSchema);
    Assert.assertNotEquals(fieldSchema.getType(), Schema.Type.NULL);

    Schema withoutUnion = AvroCompatibilityHelper.parse(TestUtil.load("HasSymbolDocs.avsc"));
    fieldSchema = AvroSchemaUtil.findNonNullUnionBranch(withoutUnion, "enumField");
    Assert.assertNotNull(fieldSchema);
    Assert.assertNotEquals(fieldSchema.getType(), Schema.Type.NULL);
  }
}
