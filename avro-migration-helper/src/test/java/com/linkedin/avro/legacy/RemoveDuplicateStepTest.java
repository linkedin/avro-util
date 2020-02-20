/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import com.linkedin.avro.TestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RemoveDuplicateStepTest {

  @Test
  public void testApplyStepsToSchema() throws Exception {
    String badSchema = TestUtil.load("WorldsMostHorrible.avsc");
    LegacySchemaTestUtil.assertValid14Schema(badSchema);

    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(badSchema, "twin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(badSchema, "innerTwin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(badSchema, "returnOfEvilTwin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(badSchema, "evilTwinsRevenge"));

    String step1 = LegacySchemaTestUtil.apply(badSchema, new RemoveDuplicateStep(null, "twin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step1, "twin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(step1, "innerTwin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(step1, "returnOfEvilTwin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(step1, "evilTwinsRevenge"));

    String step2 = LegacySchemaTestUtil.apply(step1, new RemoveDuplicateStep(null, "innerTwin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step2, "twin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step2, "innerTwin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(step2, "returnOfEvilTwin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(step2, "evilTwinsRevenge"));

    String step3 = LegacySchemaTestUtil.apply(step2, new RemoveDuplicateStep(null, "returnOfEvilTwin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step3, "twin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step3, "innerTwin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step3, "returnOfEvilTwin"));
    Assert.assertEquals(2, LegacySchemaTestUtil.countMatches(step3, "evilTwinsRevenge"));

    String step4 = LegacySchemaTestUtil.apply(step3, new RemoveDuplicateStep(null, "evilTwinsRevenge"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step4, "twin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step4, "innerTwin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step4, "returnOfEvilTwin"));
    Assert.assertEquals(1, LegacySchemaTestUtil.countMatches(step4, "evilTwinsRevenge"));
  }


}
