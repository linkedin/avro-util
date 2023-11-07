/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import java.lang.reflect.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificData;


/**
 * utility code specific to avro 1.11
 */
public class Avro111Utils {

  private final static boolean IS_AT_LEAST_1_11_1;
  private final static boolean IS_AT_LEAST_1_11_2;
  private final static boolean IS_AT_LEAST_1_11_3;

  static {
    Class<?>[] inners = GenericDatumReader.class.getDeclaredClasses(); //never null
    boolean found = false;
    for (Class<?> innerClass : inners) {
      //inner class ReaderCache added in https://issues.apache.org/jira/browse/AVRO-3531
      //see https://github.com/apache/avro/pull/1719/files#diff-eeb81a490688bfaa5b1dfdc5dfb97d6a1c6af057f55bddc40859150b167b9336R531
      if ("ReaderCache".equals(innerClass.getSimpleName())) {
        found = true;
        break;
      }
    }
    IS_AT_LEAST_1_11_1 = found;

    found = false;
    Field[] fields = SpecificData.class.getFields(); //never null
    for (Field field : fields) {
      //introduced in https://issues.apache.org/jira/browse/AVRO-3698
      //see https://github.com/apache/avro/pull/2048/files#diff-f490a56ccb90aa34a1d14bf04baa055e10f78ba070873e6af47984de62552c20R94
      if ("RESERVED_WORD_ESCAPE_CHAR".equals(field.getName())) {
        found = true;
        break;
      }
    }
    IS_AT_LEAST_1_11_2 = found;

    found = false;
    try {
      //added in https://issues.apache.org/jira/browse/AVRO-3819
      //see https://github.com/apache/avro/pull/2432/files#diff-3d9d3ee7e05c1ae3060af2e14efdc0ef8e19b266807fc14ba41908807e258cd3R41
      Class.forName("org.apache.avro.SystemLimitException");
      found = true;
    } catch (ClassNotFoundException ignored) {
      //expected in avro < 1.11.3
    }
    IS_AT_LEAST_1_11_3 = found;
  }

  private Avro111Utils() {
    //util class
  }

  public static boolean isAtLeast1111() {
    return IS_AT_LEAST_1_11_1;
  }

  public static boolean isAtLeast1112() {
    return IS_AT_LEAST_1_11_2;
  }

  public static boolean isAtLeast1113() {
    return IS_AT_LEAST_1_11_3;
  }
}
