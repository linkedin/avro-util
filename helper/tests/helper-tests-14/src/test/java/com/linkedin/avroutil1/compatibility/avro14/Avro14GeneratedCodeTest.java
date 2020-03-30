/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import org.testng.annotations.Test;


public class Avro14GeneratedCodeTest {

  @Test(expectedExceptions = ExceptionInInitializerError.class)
  public void demonstrateBadCodeForMultilineDocs() {
    new by14.RecordWithMultilineDoc();
  }
}
