/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.util;

import java.util.stream.IntStream;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * This is to test {@link StreamUtil}
 */
public class StreamUtilTest {
  @Test
  public void testParallelStreaming() throws Exception {
    int result = IntStream.rangeClosed(1, 100)
        .boxed()
        .collect(StreamUtil.toParallelStream(x -> x * x, 3, 4))
        .reduce(0, Integer::sum);

    int expected = IntStream.rangeClosed(1, 100).map(x -> x * x).sum();

    Assert.assertEquals(result, expected);
  }
}
