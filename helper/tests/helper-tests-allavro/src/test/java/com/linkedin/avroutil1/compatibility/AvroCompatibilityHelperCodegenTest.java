/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.testng.annotations.Test;


/**
 * tests code generation transformations
 */
public class AvroCompatibilityHelperCodegenTest {

  @Test
  public void testRecordWithMultilineDocsUnder14Usable() {
    new under14.RecordWithMultilineDoc();
  }

  @Test
  public void testRecordWithMultilineDocsUnder15Usable() {
    new under15.RecordWithMultilineDoc();
  }

  @Test
  public void testRecordWithMultilineDocsUnder16Usable() {
    new under16.RecordWithMultilineDoc();
  }

  @Test
  public void testRecordWithMultilineDocsUnder17Usable() {
    new under17.RecordWithMultilineDoc();
  }

  @Test
  public void testRecordWithMultilineDocsUnder18Usable() {
    new under18.RecordWithMultilineDoc();
  }

  @Test
  public void testRecordWithMultilineDocsUnder19Usable() {
    new under19.RecordWithMultilineDoc();
  }
}
