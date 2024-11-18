/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro111UtilsTest {

  @Test
  public void testAvro1111Detection() throws Exception {
    //at time of writing we use 1.11.4
    Assert.assertTrue(Avro111Utils.isAtLeast1111());
  }

  @Test
  public void testAvro1112Detection() throws Exception {
    //at time of writing we use 1.11.4
    Assert.assertTrue(Avro111Utils.isAtLeast1112());
  }

  @Test
  public void testAvro1113Detection() throws Exception {
    //at time of writing we use 1.11.4
    Assert.assertTrue(Avro111Utils.isAtLeast1113());
  }

  @Test
  public void testAvro1114Detection() throws Exception {
    //at time of writing we use 1.11.4
    Assert.assertTrue(Avro111Utils.isAtLeast1114());
  }
}
