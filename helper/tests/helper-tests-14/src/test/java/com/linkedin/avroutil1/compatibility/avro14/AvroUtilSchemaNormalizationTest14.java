/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.normalization.AvroUtilSchemaNormalization;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import java.util.Collections;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroUtilSchemaNormalizationTest14 {

  @Test
  public void testCanonicalize() throws IOException {
    Schema schema = Schema.parse(TestUtil.load("MonsantoRecord.avsc"));
    Assert.assertThrows(UnsupportedOperationException.class,
        () -> AvroUtilSchemaNormalization.getCanonicalForm(schema, AvscGenerationConfig.CANONICAL_ONELINE,
            Collections.EMPTY_LIST));
  }

}
