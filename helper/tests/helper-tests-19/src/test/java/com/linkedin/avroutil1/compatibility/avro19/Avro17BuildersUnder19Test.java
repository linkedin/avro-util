/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import org.testng.Assert;
import org.testng.annotations.Test;
import under17target17.SimpleRecord;


public class Avro17BuildersUnder19Test {

  @Test
  public void testBuilders() {
    SimpleRecord instance = SimpleRecord.newBuilder().setStringField("woohoo").build();
    Assert.assertEquals("woohoo", instance.stringField);
  }
}
