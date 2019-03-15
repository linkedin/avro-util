/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro;

import java.io.IOException;
import org.apache.commons.io.IOUtils;


public class TestUtil {
  private TestUtil() {
    //util
  }

  public static String load(String path) throws IOException {
    return IOUtils.toString(Thread.currentThread().getContextClassLoader().getResourceAsStream(path), "utf-8");
  }
}
