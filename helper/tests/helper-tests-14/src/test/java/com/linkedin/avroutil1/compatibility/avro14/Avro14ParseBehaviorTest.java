/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro14ParseBehaviorTest {

  @Test
  public void demonstrateAvro14DoesntValidateRecordNamespaces() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidNamespace.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro14DoesntValidateRecordNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidName.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro14DoesntValidateFieldNames() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldName.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro14DoesntValidateFieldDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidFieldDefaultValue.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro14DoesntValidateUnionDefaults() throws Exception {
    String avsc = TestUtil.load("RecordWithInvalidUnionDefault.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro14DoesntValidateFixedNames() throws Exception {
    String avsc = TestUtil.load("FixedWithInvalidName.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro14DoesntValidateEnumNames() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidName.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }

  @Test
  public void demonstrateAvro14DoesntValidateEnumValues() throws Exception {
    String avsc = TestUtil.load("EnumWithInvalidValue.avsc");
    Schema parsedByVanilla = Schema.parse(avsc);
    Assert.assertNotNull(parsedByVanilla);
  }
}
