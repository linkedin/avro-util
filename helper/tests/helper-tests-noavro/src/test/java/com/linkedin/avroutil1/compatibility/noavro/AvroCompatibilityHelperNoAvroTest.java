/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.noavro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperNoAvroTest {

  @Test
  public void testAvroVersionDetection() throws Exception {
    AvroVersion detected = AvroCompatibilityHelper.getRuntimeAvroVersion();
    Assert.assertNull(detected, "expected null, got " + detected);
  }
}
