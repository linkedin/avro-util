/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.testcommon.TestUtil;
import net.openhft.compiler.CompilerUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro14FutureGeneratedCodeAtCompileTest {

  @Test
  public void demonstrateAvro19CompatibleCode() throws Exception {
    String sourceCode = TestUtil.load("under19/SimpleRecord.java");
    Class<?> transformedClass = CompilerUtils.CACHED_COMPILER.loadFromJava("under19.SimpleRecord", sourceCode);
    Assert.assertNotNull(transformedClass);
  }
}
