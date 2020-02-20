/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;


/**
 * this class represents an output java file from avro code generation
 * it is required because the real underlying class has changed
 * across avro versions
 */
public class AvroGeneratedSourceCode {
  private final String path;
  private final String contents;

  public AvroGeneratedSourceCode(String path, String contents) {
    this.path = path;
    this.contents = contents;
  }

  /**
   * @return the path (relative to source root) under which this file is intended to be written
   */
  public String getPath() {
    return path;
  }

  /**
   * @return the contents of the file
   */
  public String getContents() {
    return contents;
  }

  public String getFullyQualifiedClassName() {
    //avro leaves us little choice but to "guess" the FQCN from the file name
    return path.replaceAll("/", ".").replace(".java", "");
  }

  /**
   * writes the contents into a file at destDir/path. any existing
   * file will be deleted and overwritten. files are always written using UTF-8
   * @param destDir root folder under which path will be created
   * @return the file created
   * @throws IOException in io errors
   */
  public File writeToDestination(File destDir) throws IOException {
    File f = new File(destDir, path);
    File folder = f.getParentFile();
    if (!folder.exists() && !folder.mkdirs()) {
      throw new IllegalStateException("unable to create path " + folder);
    }
    if (f.exists() && !f.delete()) {
      throw new IllegalStateException("unable to delete existing file " + f);
    }
    try (Writer writer = new OutputStreamWriter(new FileOutputStream(f), StandardCharsets.UTF_8)) {
      writer.write(contents);
    }
    return f;
  }

  @Override
  public String toString() {
    return path;
  }
}
