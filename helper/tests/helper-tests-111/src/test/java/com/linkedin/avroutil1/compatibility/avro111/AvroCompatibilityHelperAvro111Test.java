/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperAvro111Test {

  @Test
  public void testAvroVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_11;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }

  @Test
  public void testAvroCompilerVersionDetection() {
    AvroVersion expected = AvroVersion.AVRO_1_11;
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroCompilerVersion();
    Assert.assertEquals(detected, expected, "expected " + expected + ", got " + detected);
  }
}
