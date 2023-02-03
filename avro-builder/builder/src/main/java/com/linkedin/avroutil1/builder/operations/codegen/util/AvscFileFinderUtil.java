/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.util;

import com.linkedin.avroutil1.builder.BuilderConsts;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.commons.io.FileUtils;


public class AvscFileFinderUtil {
  private static final String[] EXTENSIONS = new String[]{BuilderConsts.AVSC_EXTENSION};
  private static final String TEMP_DIR_NAME = "tempCompressedAvroSrc";

  private AvscFileFinderUtil() {
  }

  public static Collection<File> findFiles(File inputRoot) throws IOException {
    Set<File> avscFiles = new HashSet<>();
    if (inputRoot.isDirectory()) {
      avscFiles.addAll(FileUtils.listFiles(inputRoot, EXTENSIONS, true));
    } else if (inputRoot.getAbsolutePath().endsWith(BuilderConsts.DOT + BuilderConsts.ZIP_EXTENSION)) {
      avscFiles.addAll(findSchemasFromCompressedFile(inputRoot, false));
    } else if (inputRoot.getAbsolutePath().endsWith(BuilderConsts.DOT + BuilderConsts.JAR_EXTENSION)) {
      avscFiles.addAll(findSchemasFromCompressedFile(inputRoot, true));
    } else if (inputRoot.isFile() && inputRoot.getName().endsWith(BuilderConsts.DOT + BuilderConsts.AVSC_EXTENSION)) {
      avscFiles.add(inputRoot);
    } else {
      throw new IllegalArgumentException("input root " + inputRoot + " is not a directory, zip or jar");
    }
    return avscFiles;
  }

  private static Collection<File> findSchemasFromCompressedFile(File file, boolean isJar) throws IOException {
    Collection<File> avscFiles = new ArrayList<>();
    Path tempDir = Files.createTempDirectory(TEMP_DIR_NAME);
    try (ZipFile zipFile = isJar ? new JarFile(file) : new ZipFile(file)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();

      // ZIPs can contain duplicate entries in the manifest. Make sure to process only one.
      Set<String> processedEntries = new HashSet<>();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();

        if (processedEntries.contains(entry.getName())) {
          continue;
        }

        processedEntries.add(entry.getName());
        if (entry.getName().endsWith(BuilderConsts.DOT + BuilderConsts.AVSC_EXTENSION)) {
          File tempFile = new File(tempDir.toFile(), entry.getName());
          tempFile.getParentFile().mkdirs();
          Files.copy(zipFile.getInputStream(entry), tempFile.toPath());
          avscFiles.add(tempFile);
        }
      }
    }
    return avscFiles;
  }
}