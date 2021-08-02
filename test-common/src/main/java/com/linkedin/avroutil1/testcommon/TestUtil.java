/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.testcommon;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

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

  /**
   * finds all class files associated with a given class
   * (including any class files for inner classes)
   * @param classesRoot root of compiled code folder
   * @param clazz class to find files for
   * @return all class files for the given class and any inner classes
   */
  public static List<Path> findClassFiles(String classesRoot, Class<?> clazz) throws IOException {
    Path folder = Paths.get(classesRoot, clazz.getName().replace('.', '/')).getParent();
    Set<Path> classesToCheck = new HashSet<>();
    Pattern pattern = Pattern.compile(clazz.getSimpleName() + "(\\$\\w+)?\\.class");
    Files.walkFileTree(folder, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (!Files.isRegularFile(file)) {
          return FileVisitResult.CONTINUE;
        }
        String name = file.getName(file.getNameCount() - 1).toString();
        if (pattern.matcher(name).matches()) {
          classesToCheck.add(file);
        }
        return FileVisitResult.CONTINUE;
      }
    });
    return new ArrayList<>(classesToCheck);
  }
}
