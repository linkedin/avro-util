/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AvroSchemaBuilderUtils {

  private AvroSchemaBuilderUtils() { }

  public static List<File> toFiles(List<String> strs) {
    List<File> files = new ArrayList<>();
    for (String s : strs) {
      files.add(new File(s));
    }
    return files;
  }

  public static boolean isParentOf(File maybeParent, File maybeChild) {
    return maybeChild.toPath().startsWith(maybeParent.toPath());
  }
}
