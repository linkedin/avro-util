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

  @Test
  public void testRecordWithMultilineDocsUnder110Usable() {
    new under110.RecordWithMultilineDoc();
  }

  @Test
  public void testRecordWithMultilineDocsUnder111Usable() {
    new under111.RecordWithMultilineDoc();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder14Usable() {
    new under14.RecordWithComOrgFields();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder15Usable() {
    new under15.RecordWithComOrgFields();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder16Usable() {
    new under16.RecordWithComOrgFields();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder17Usable() {
    new under17.RecordWithComOrgFields();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder18Usable() {
    new under18.RecordWithComOrgFields();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder19Usable() {
    new under19.RecordWithComOrgFields();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder110Usable() {
    new under110.RecordWithComOrgFields();
  }

  @Test
  public void testRecordWithComOrgFieldsUnder111Usable() {
    new under111.RecordWithComOrgFields();
  }
}
