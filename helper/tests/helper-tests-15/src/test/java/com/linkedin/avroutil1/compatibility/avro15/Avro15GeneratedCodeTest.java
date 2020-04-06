/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15;

import org.testng.annotations.Test;


public class Avro15GeneratedCodeTest {

  @Test(expectedExceptions = ExceptionInInitializerError.class)
  public void demonstrateBadCodeForMultilineDocs() {
    new by15.RecordWithMultilineDoc();
  }
}
