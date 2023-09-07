/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import com.linkedin.avroutil1.compatibility.CompatibleSpecificRecordBuilderBase;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class CompatibleSpecificRecordBuilderBaseTest {

  @Test
  public void testValidate() {
    Schema.Field field = new Schema.Field("field1", Schema.create(Schema.Type.STRING), "field doc", "null");
    Schema myRecord = Schema.createRecord(Arrays.asList(field));
    new MyBuilderClass(myRecord).testValidate(field, null);
  }

  class MyBuilderClass extends CompatibleSpecificRecordBuilderBase {

    protected MyBuilderClass(Schema schema) {
      super(schema);
    }

    public void testValidate(Schema.Field f, Object o) {
      validate(f, o);
    }
  }
}