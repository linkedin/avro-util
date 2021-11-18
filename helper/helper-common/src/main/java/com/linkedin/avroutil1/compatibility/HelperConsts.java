/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HelperConsts {
  public final static Set<Schema.Type> NAMED_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          Schema.Type.RECORD,
          Schema.Type.ENUM,
          Schema.Type.FIXED
  )));

  public static final String HELPER_SIMPLE_NAME = "AvroCompatibilityHelper";
  public static final String HELPER_FQCN = "com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper";

  private HelperConsts() {
    //nope
  }
}
