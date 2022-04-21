/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.testng.Assert;
import org.testng.annotations.Test;


public class CodeTransformationsTest {

  @Test
  public void testFindEndOfSchemaDeclaration() {
    String normal = "public static final org.apache.avro.Schema SCHEMA$ = whatever(\"{json}\"); fluff";
    String huge = "public static final org.apache.avro.Schema SCHEMA$ = whatever(new StringBuilder().append(\"{json}\").append(\"{more json}\").toString());";

    Assert.assertEquals(CodeTransformations.findEndOfSchemaDeclaration(normal), normal.length() - 6);
    Assert.assertEquals(CodeTransformations.findEndOfSchemaDeclaration(huge), huge.length());
  }
}
