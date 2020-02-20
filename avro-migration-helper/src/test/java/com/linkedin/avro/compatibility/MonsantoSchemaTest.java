/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import com.linkedin.avro.TestUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


public class MonsantoSchemaTest {

  //this demonstrates an issue with avro 1.4 that causes schemas corruption.
  //the issue is that a union field in emitted with a simple class name that
  //is not found when parsing, because its in a different namespace (class X in the example)
  @Test(expectedExceptions = SchemaParseException.class)
  public void demonstrateBadSchema() throws Exception {
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (runtimeVersion != AvroVersion.AVRO_1_4) {
      throw new SkipException("only supported under " + AvroVersion.AVRO_1_4 + ". runtime version detected as " + runtimeVersion);
    }
    String originalAvsc = TestUtil.load("BadInnerNamespace.avsc");
    Schema parsed = Schema.parse(originalAvsc);
    String toStringOutput = parsed.toString();
    Schema.parse(toStringOutput);
    Assert.fail("2nd parse expected to throw");
  }
}
