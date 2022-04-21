/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

public class SourceCodeUtilsTest {

    @Test
    public void testSafeSplit() {
        Assert.assertEquals(
                Arrays.asList("1234567890", "abcdefghij"),
                SourceCodeUtils.safeSplit("1234567890abcdefghij", 10));
        Assert.assertEquals(
                Arrays.asList("1234567890", "abcdefghij", "AB"),
                SourceCodeUtils.safeSplit("1234567890abcdefghijAB", 10));
        Assert.assertEquals(Collections.singletonList("1234567890"),
                SourceCodeUtils.safeSplit("1234567890", 10));
        //dont chop at '
        Assert.assertEquals(
                Arrays.asList("12345678", "9'abcdefgh", "ij"),
                SourceCodeUtils.safeSplit("123456789'abcdefghij", 10));
        //unicode escapes not on the boundary
        Assert.assertEquals(
                Arrays.asList("xx\\u1234xx", "xxxxxxxxxx"),
                SourceCodeUtils.safeSplit("xx\\u1234xxxxxxxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxx\\u1234", "xxxxxxxxxx"),
                SourceCodeUtils.safeSplit("xxxx\\u1234xxxxxxxxxx", 10));
        //unicode escapes cross the boundary
        Assert.assertEquals(
                Arrays.asList("xxxx","x\\u1234xxx", "xxxxxx"),
                SourceCodeUtils.safeSplit("xxxxx\\u1234xxxxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxx","x\\u1234xxx", "xxxxx"),
                SourceCodeUtils.safeSplit("xxxxxx\\u1234xxxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxx","x\\u1234xxx", "xxxx"),
                SourceCodeUtils.safeSplit("xxxxxxx\\u1234xxxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxx","x\\u1234xxx", "xxx"),
                SourceCodeUtils.safeSplit("xxxxxxxx\\u1234xxxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxx","x\\u1234xxx", "xx"),
                SourceCodeUtils.safeSplit("xxxxxxxxx\\u1234xxxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
                SourceCodeUtils.safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxxx","x\\u1234xxx", "x"),
                SourceCodeUtils.safeSplit("xxxxxxxxxx\\u1234xxxx", 10));
        Assert.assertEquals(
                Arrays.asList("xxxxxxxxxx","x\\u1234xxx"),
                SourceCodeUtils.safeSplit("xxxxxxxxxxx\\u1234xxx", 10));
    }
}
