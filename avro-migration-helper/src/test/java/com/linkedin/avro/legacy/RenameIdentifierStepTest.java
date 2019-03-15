/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import com.linkedin.avro.TestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.avro.legacy.LegacySchemaTestUtil.*;


public class RenameIdentifierStepTest {

  @Test
  public void testApplyStepToSchema() throws Exception {
    String badSchema = TestUtil.load("WorldsMostHorrible.avsc");
    assertValid14Schema(badSchema);
    Assert.assertEquals(1, countMatches(badSchema, "NOT OK"));
    Assert.assertEquals(0, countMatches(badSchema, "somethingElse"));

    String fixedSchema = apply(badSchema, new RenameIdentifierStep(null, "NOT OK", "somethingElse"));

    Assert.assertEquals(0, countMatches(fixedSchema, "NOT OK"));
    Assert.assertEquals(1, countMatches(fixedSchema, "somethingElse"));
  }
}
