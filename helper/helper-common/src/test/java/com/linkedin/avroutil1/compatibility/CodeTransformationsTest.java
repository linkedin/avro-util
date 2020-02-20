/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CodeTransformationsTest {

  @Test
  public void testSafeSplit() {
    Assert.assertEquals(
        Arrays.asList("1234567890", "abcdefghij"),
        CodeTransformations.safeSplit("1234567890abcdefghij", 10));
    Assert.assertEquals(
        Arrays.asList("1234567890", "abcdefghij", "AB"),
        CodeTransformations.safeSplit("1234567890abcdefghijAB", 10));
    Assert.assertEquals(Collections.singletonList("1234567890"),
        CodeTransformations.safeSplit("1234567890", 10));
    //dont chop at '
    Assert.assertEquals(
        Arrays.asList("12345678", "9'abcdefgh", "ij"),
        CodeTransformations.safeSplit("123456789'abcdefghij", 10));
    //unicode escapes not on the boundary
    Assert.assertEquals(
        Arrays.asList("xx\\u1234xx", "xxxxxxxxxx"),
        CodeTransformations.safeSplit("xx\\u1234xxxxxxxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxx\\u1234", "xxxxxxxxxx"),
        CodeTransformations.safeSplit("xxxx\\u1234xxxxxxxxxx", 10));
    //unicode escapes cross the boundary
    Assert.assertEquals(
        Arrays.asList("xxxx","x\\u1234xxx", "xxxxxx"),
        CodeTransformations.safeSplit("xxxxx\\u1234xxxxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxx","x\\u1234xxx", "xxxxx"),
        CodeTransformations.safeSplit("xxxxxx\\u1234xxxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxx","x\\u1234xxx", "xxxx"),
        CodeTransformations.safeSplit("xxxxxxx\\u1234xxxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxx","x\\u1234xxx", "xxx"),
        CodeTransformations.safeSplit("xxxxxxxx\\u1234xxxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxx","x\\u1234xxx", "xx"),
        CodeTransformations.safeSplit("xxxxxxxxx\\u1234xxxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
        CodeTransformations.safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
        CodeTransformations.safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
    Assert.assertEquals(
        Arrays.asList("xxxxxxxxxx","x\\u1234xxx"),
        CodeTransformations.safeSplit("xxxxxxxxxxx\\u1234xxx", 10));
  }
}
