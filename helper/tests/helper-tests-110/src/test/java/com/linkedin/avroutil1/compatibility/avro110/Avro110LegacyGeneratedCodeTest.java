/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import org.testng.annotations.Test;


public class Avro110LegacyGeneratedCodeTest {

  @Test(expectedExceptions = AbstractMethodError.class)
  public void demonstrateAvro14FixedUnusableUnder110() throws Exception {
    //avro fixed classes extend org.apache.avro.specific.SpecificFixed which, in turn implements
    //org.apache.avro.generic.GenericFixed. in avro 1.5+ GenericFixed extends org.apache.avro.generic.GenericContainer.
    //GenericContainer, in turn, defined method getSchema() that avro-14-generated fixed classes dont implement
    new by14.SimpleFixed();
  }

}
