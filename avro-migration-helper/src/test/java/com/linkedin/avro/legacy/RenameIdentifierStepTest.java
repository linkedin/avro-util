/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import com.linkedin.avro.TestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RenameIdentifierStepTest {

  @Test
  public void testApplyStepToSchema() throws Exception {
    String badSchema = TestUtil.load("WorldsMostHorrible.avsc");
    LegacySchemaTestUtil.assertValid14Schema(badSchema);
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(badSchema, "NOT OK"));
    Assert.assertEquals(0, LegacySchemaTestUtil.countMatches(badSchema, "somethingElse"));

    String fixedSchema = LegacySchemaTestUtil.apply(badSchema, new RenameIdentifierStep(null, "NOT OK", "somethingElse"));

    Assert.assertEquals(0, LegacySchemaTestUtil.countMatches(fixedSchema, "NOT OK"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(fixedSchema, "somethingElse"));
  }
}
