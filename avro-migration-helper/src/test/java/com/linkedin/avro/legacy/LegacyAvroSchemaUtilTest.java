/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import org.testng.Assert;
import org.testng.annotations.Test;


public class LegacyAvroSchemaUtilTest {

  @Test
  public void testEscapeIllegalCharacters() throws Exception {
    Assert.assertEquals("ho_32_no", LegacyAvroSchemaUtil.escapeIllegalCharacters("ho no"));
    Assert.assertEquals("hell_45_no", LegacyAvroSchemaUtil.escapeIllegalCharacters("hell-no"));
    Assert.assertEquals("_64_never", LegacyAvroSchemaUtil.escapeIllegalCharacters("@never"));
  }
}
