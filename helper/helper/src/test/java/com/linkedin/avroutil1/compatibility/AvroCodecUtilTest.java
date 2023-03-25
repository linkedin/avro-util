/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class AvroCodecUtilTest {

  @DataProvider
  private Object[][] TestBytesLeftoverJsonProvider() {
    return new Object[][]{
        {"", false},
        {"    ", false},
        {"   \t  \n", false},
        {"}", false},
        {"\t} ", false},
        {"}\n", false},
        {"}}", true},
        {"} }\n", true},
        {"} \t a b\n", true},
        {"a", true},
        {"a b\t  \n", true}
    };
  }

  @Test(dataProvider = "TestBytesLeftoverJsonProvider")
  public void testBytesLeftoverJson(String leftover, boolean expectedCheckResult) throws Exception {
    InputStream inputStream = new ByteArrayInputStream(leftover.getBytes(StandardCharsets.UTF_8));

    Assert.assertEquals(AvroCodecUtil.bytesLeftoverJson(inputStream), expectedCheckResult);
  }
}
