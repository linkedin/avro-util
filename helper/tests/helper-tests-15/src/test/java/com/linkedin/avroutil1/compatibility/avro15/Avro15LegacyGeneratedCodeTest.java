/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import com.google.common.base.Throwables;
import org.apache.avro.AvroRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro15LegacyGeneratedCodeTest {

  @Test
  public void demonstrateAvro14FixedUnusableUnder15() throws Exception {
    //avro fixed classes extend org.apache.avro.specific.SpecificFixed which, in turn implements
    //org.apache.avro.generic.GenericFixed. in avro 1.5+ GenericFixed extends org.apache.avro.generic.GenericContainer.
    //GenericContainer, in turn, defined method getSchema() that avro-14-generated fixed classes dont implement.
    //under 1.5 specifically the failure is a little different - its looking for field SCHEMA$ directly 1st
    try {
      new by14.SimpleFixed();
      Assert.fail("expected to throw");
    } catch (AvroRuntimeException issue) {
      Throwable root = Throwables.getRootCause(issue);
      Assert.assertTrue(root instanceof NoSuchFieldException);
      Assert.assertTrue(root.getMessage().contains("SCHEMA$"));
    }
  }
}
