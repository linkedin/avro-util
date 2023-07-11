/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1;

import org.testng.Assert;
import org.testng.annotations.Test;


public class EnumsTest {
  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void testGetEnumConstantWithNegativeOrdinal() {
    Enums.getConstant(Thread.State.class, -1);
  }

  @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
  public void testGetEnumConstantWithLargeOrdinal() {
    Enums.getConstant(Thread.State.class, Thread.State.values().length);
  }

  @Test
  public void testGetEnumConstant() {
    for (Thread.State state : Thread.State.values()) {
      Assert.assertEquals(Enums.getConstant(Thread.State.class, state.ordinal()), state);
    }
  }
}
