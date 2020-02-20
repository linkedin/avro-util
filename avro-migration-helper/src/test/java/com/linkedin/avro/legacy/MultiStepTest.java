/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import com.linkedin.avro.TestUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;


public class MultiStepTest {

  @Test
  public void testCompleteFix() throws Exception {
    String badSchema = TestUtil.load("WorldsMostHorrible.avsc");
    LegacySchemaTestUtil.assertValid14Schema(badSchema);

    List<SchemaTransformStep> steps = new ArrayList<>(Arrays.asList(
        new RemoveDuplicateStep(null, "twin"),
        new RemoveDuplicateStep(null, "innerTwin"),
        new RemoveDuplicateStep(null, "returnOfEvilTwin"),
        new RemoveDuplicateStep(null, "evilTwinsRevenge"),
        new RenameIdentifierStep(null, "NOT OK", "OK_NOW")
    ));

    long seed = System.currentTimeMillis();
    Random rng = new Random(seed);
    Collections.shuffle(steps, rng); //make it interesting

    try {
      String fixed = LegacySchemaTestUtil.apply(badSchema, steps);
      LegacySchemaTestUtil.assertValidSchema(fixed);
    } catch (Exception e) {
      throw new IllegalStateException("test failed. seed is " + seed, e);
    }
  }
}
