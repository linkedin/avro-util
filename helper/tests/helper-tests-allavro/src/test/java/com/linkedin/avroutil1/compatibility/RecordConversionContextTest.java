/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RecordConversionContextTest {

  @Test
  public void testClassLookup() throws Exception {

    RecordConversionContext ctx = new RecordConversionContext(new RecordConversionConfig(
        false, false, false, true, StringRepresentation.Utf8, false
    ));
    ctx.setClassLoader(Thread.currentThread().getContextClassLoader());

    Class<?> clazz = ctx.load(
        String.class.getName(),
        null,
        false
    );
    Assert.assertSame(clazz, String.class);
    clazz = ctx.load(
        "DoesNotExist" + UUID.randomUUID(), //wont be found
        Collections.singletonList(String.class.getName()), //but has aliases
        false
    );
    Assert.assertSame(clazz, String.class); //resolved via alias
    clazz = ctx.load(
        "DoesNotExist" + UUID.randomUUID(),
        Arrays.asList(String.class.getName(), Boolean.class.getName()),
        false
    );
    Assert.assertSame(clazz, String.class); //1st alias
    try {
      ctx.load(
          "DoesNotExist" + UUID.randomUUID(),
          Arrays.asList(String.class.getName(), Boolean.class.getName()),
          true //require only 1 alias to be found
      );
      Assert.fail("expected to throw");
    } catch (IllegalStateException expected) {
      //expected
    }
  }
}
