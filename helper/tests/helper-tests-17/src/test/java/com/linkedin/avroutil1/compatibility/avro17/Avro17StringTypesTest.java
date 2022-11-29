/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.AvroCodecUtil;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;
import under14.RecordWithStrings;


public class Avro17StringTypesTest {

  @Test
  public void testDefaultStringRepresentation() throws Exception {
    RecordWithStrings original = new RecordWithStrings();
    original.f1 = "string";
    original.f2 = new Utf8("utf8");
    original.f3 = new Utf8("f3");
    original.f4 = "f4";

    byte[] bytes = AvroCodecUtil.serializeBinary(original);
    GenericRecord deserialized1 = AvroCodecUtil.deserializeAsGeneric(
        bytes,
        RecordWithStrings.getClassSchema(),
        RecordWithStrings.getClassSchema()
    );

    RecordWithStrings deserialized2 = AvroCodecUtil.deserializeAsSpecific(
        bytes,
        RecordWithStrings.getClassSchema(),
        RecordWithStrings.class
    );

    //avro 1.7 just puts Utf8s everywhere
    Assert.assertTrue(deserialized1.get("f1") instanceof Utf8);
    Assert.assertTrue(deserialized1.get("f2") instanceof Utf8);
    Assert.assertTrue(deserialized2.f1 instanceof Utf8);
    Assert.assertTrue(deserialized2.f2 instanceof Utf8);
    //but respects logical types
    Assert.assertTrue(deserialized1.get("f3") instanceof String);
    Assert.assertTrue(deserialized1.get("f4") instanceof Utf8);
    Assert.assertTrue(deserialized2.f3 instanceof String);
    Assert.assertTrue(deserialized2.f4 instanceof Utf8);
  }
}
