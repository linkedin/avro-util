/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AvscWriterTest {

  @Test
  public void testEnumWriting() throws Exception {
    String originalAvsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema parsed = Schema.parse(originalAvsc);
    String avsc = AvroCompatibilityHelper.toAvsc(parsed, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY);
    JsonAssertions.assertThatJson(avsc).isObject().containsEntry("default", "B");
  }
}
