/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro111UtilsUnder1110Test {

  @Test
  public void testAvro1111Detection() throws Exception {
    //this module depends on 1.11.0
    Assert.assertFalse(Avro111Utils.isAtLeast1111());
  }

  @Test
  public void testAvro1112Detection() throws Exception {
    //this module depends on 1.11.0
    Assert.assertFalse(Avro111Utils.isAtLeast1112());
  }

  @Test
  public void testAvro1113Detection() throws Exception {
    //this module depends on 1.11.0
    Assert.assertFalse(Avro111Utils.isAtLeast1113());
  }
}
