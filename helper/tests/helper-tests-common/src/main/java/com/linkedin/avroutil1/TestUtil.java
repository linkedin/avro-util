/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import org.apache.commons.io.IOUtils;


public class TestUtil {

  private TestUtil() {
    //util
  }

  public static String load(String path) throws IOException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    if (is == null) {
      throw new IllegalArgumentException("resource " + path + " not found on context classloader");
    }
    try {
      return IOUtils.toString(is, "utf-8");
    } finally {
      is.close();
    }
  }

  public static byte[] loadBinary(String path) throws IOException {
    InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    if (is == null) {
      throw new IllegalArgumentException("resource " + path + " not found on context classloader");
    }
    try {
      return IOUtils.toByteArray(is);
    } finally {
      is.close();
    }
  }

  public static boolean compilerExpectedOnClasspath() {
    //when we create the gradle test tasks we set a system property ("runtime.avro.version")
    //with the name of the gradle configuration used for avro. those that have "NoCompiler"
    //in them are expected NOT to include the avro-compiler jar
    Properties sysProps = System.getProperties();
    if (!sysProps.containsKey("runtime.avro.version")) {
      throw new AssertionError("system properties do not container key \"runtime.avro.version\"");
    }
    String avroVersion = sysProps.getProperty("runtime.avro.version");
    if (avroVersion == null) {
      throw new AssertionError("value under system property \"runtime.avro.version\" is null");
    }
    String trimmed = avroVersion.trim().toLowerCase(Locale.ROOT);
    if (trimmed.isEmpty()) {
      throw new AssertionError("value under system property \"runtime.avro.version\" is empty");
    }
    return !trimmed.contains("nocompiler");
  }

  public static Path getNewFile(Path folder, String fileName) throws IOException {
    if (!Files.exists(folder)) {
      Files.createDirectories(folder);
    }
    if (!Files.isDirectory(folder) || !Files.isWritable(folder)) {
      throw new IllegalStateException("root folder " + folder + " not a directory or isnt writable");
    }
    Path file = folder.resolve(fileName);
    if (Files.exists(file)) {
      if (!Files.isRegularFile(file)) {
        throw new IllegalStateException("was expecting " + file + " to be a regular file");
      }
      Files.delete(file);
    }
    Files.createFile(file);
    return file;
  }
}
