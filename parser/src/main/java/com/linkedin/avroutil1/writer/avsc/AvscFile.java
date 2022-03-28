/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.writer.avsc;

import com.linkedin.avroutil1.model.AvroSchema;
import java.nio.file.Path;


public class AvscFile {
  public static final String SUFFIX = "avsc";

  private AvroSchema topLevelSchema;
  private Path pathFromRoot;
  private String contents;

  public AvscFile(AvroSchema topLevelSchema, Path pathFromRoot, String contents) {
    this.topLevelSchema = topLevelSchema;
    this.pathFromRoot = pathFromRoot;
    this.contents = contents;
  }

  public AvroSchema getTopLevelSchema() {
    return topLevelSchema;
  }

  public Path getPathFromRoot() {
    return pathFromRoot;
  }

  public String getContents() {
    return contents;
  }
}
