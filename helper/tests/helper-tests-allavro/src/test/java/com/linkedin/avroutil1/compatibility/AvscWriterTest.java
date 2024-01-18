/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import net.javacrumbs.jsonunit.assertj.JsonAssertions;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class AvscWriterTest {

  @Test
  public void testEnumToAvsc() throws Exception {
    String originalAvsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema parsed = Schema.parse(originalAvsc);
    String avsc = AvroCompatibilityHelper.toAvsc(parsed, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY);
    JsonAssertions.assertThatJson(avsc).isObject().containsEntry("default", "B");
  }

  @Test
  public void testEnumWriteAvscWriter() throws Exception {
    String originalAvsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema parsed = Schema.parse(originalAvsc);
    StringWriter writer = new StringWriter();
    AvroCompatibilityHelper.writeAvsc(parsed, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY, writer);
    String avsc = writer.toString();
    JsonAssertions.assertThatJson(avsc).isObject().containsEntry("default", "B");
  }

  @Test
  public void testEnumWriteAvscOutputStream() throws Exception {
    String originalAvsc = TestUtil.load("PerfectlyNormalEnum.avsc");
    Schema parsed = Schema.parse(originalAvsc);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    AvroCompatibilityHelper.writeAvsc(parsed, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY, outputStream);
    String avsc = outputStream.toString(StandardCharsets.UTF_8.name());
    JsonAssertions.assertThatJson(avsc).isObject().containsEntry("default", "B");
  }
}
