/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.testng.annotations.Test;

/**
 * tests props-related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroCompatibilityHelperPropsTest {

  @Test
  public void testPropSetting() throws Exception {
    Schema schema = Schema.parse(TestUtil.load("PerfectlyNormalRecord.avsc"));
  }
}
