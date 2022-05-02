/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.builder;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

public class SpecificCompilerHelper {

  private SpecificCompilerHelper() { }

  public static List<File> compile(List<Schema> schemas, File outputDir) {
    return compile(schemas, outputDir, AvroVersion.AVRO_1_4, Collections.emptyList(), Collections.emptyList(), CodeGenerationConfig.COMPATIBLE_DEFAULTS);
  }

  public static List<File> compile(List<Schema> schemas, File outputDir, AvroVersion minTargetVersion) {
    return compile(schemas, outputDir, minTargetVersion, Collections.emptyList(), Collections.emptyList(), CodeGenerationConfig.COMPATIBLE_DEFAULTS);
  }

  public static List<File> compile(
      List<Schema> schemas,
      File outputDir,
      AvroVersion minTargetVersion,
      Collection<String> toSkip,
      Collection<String> ignoreDups
  ) {
    return compile(schemas, outputDir, minTargetVersion, toSkip, ignoreDups, CodeGenerationConfig.COMPATIBLE_DEFAULTS);
  }

  public static List<File> compile(
      List<Schema> schemas,
      File outputDir,
      AvroVersion minTargetVersion,
      Collection<String> toSkip,
      Collection<String> ignoreDups,
      CodeGenerationConfig codeGenerationConfig
  ) {
    return compile(
        schemas,
        outputDir,
        minTargetVersion,
        toSkip,
        ignoreDups,
        DuplicateSchemaBehaviour.FAIL_IF_DIFFERENT,
        codeGenerationConfig
    );
  }

  public static List<File> compile(
      List<Schema> schemas,
      File outputDir,
      AvroVersion minTargetVersion,
      Collection<String> toSkip,
      Collection<String> ignoreDups,
      DuplicateSchemaBehaviour onDups,
      CodeGenerationConfig codeGenerationConfig
  ) {
    if (outputDir == null) {
      throw new IllegalArgumentException("must provide output root dir");
    }
    if (schemas == null || schemas.isEmpty()) {
      return Collections.emptyList();
    }
    Collection<AvroGeneratedSourceCode> generatedClasses = AvroCompatibilityHelper.compile(
        schemas, minTargetVersion, AvroVersion.latest(), codeGenerationConfig
    );
    try {
      Map<File, AvroGeneratedSourceCode> generatedAlready = new HashMap<>(generatedClasses.size());
      for (AvroGeneratedSourceCode generatedClass : generatedClasses) {
        String fqcn = generatedClass.getFullyQualifiedClassName();
        if (toSkip != null && !toSkip.isEmpty()) {
          if (toSkip.contains(fqcn)) {
            continue;
          }
        }
        File absoluteFile = new File(outputDir, generatedClass.getPath()).getCanonicalFile(); //remove any ".." shenanigans
        AvroGeneratedSourceCode conflictingDefinition = generatedAlready.put(absoluteFile, generatedClass);
        if (conflictingDefinition != null) {
          if (conflictingDefinition.getContents().equals(generatedClass.getContents())) {
            //dups are identical
            String cause = "avro type " + absoluteFile.getAbsolutePath() + " defined (identically) multiple times";
            if (onDups == DuplicateSchemaBehaviour.FAIL && !ignoreDups.contains(fqcn)) {
              throw new IllegalStateException("ERROR: " + cause);
            } else {
              System.err.println("WARNING: " + cause);
            }
            //TODO - clean these dups out
            System.err.println("WARNING: avro type " + absoluteFile.getAbsolutePath() + " defined (identically) multiple times");
          } else {
            //definition is different in 2+ locations
            String cause = String.format(
                "avro type %s defined DIFFERENTLY multiple times: %s vs %s",
                absoluteFile.getAbsolutePath(),
                conflictingDefinition.getContents(),
                generatedClass.getContents()
            );
            if (onDups == DuplicateSchemaBehaviour.WARN) {
              System.err.println("ERROR (suppressed): " + cause);
            } else if (!ignoreDups.contains(fqcn)) {
              throw new IllegalStateException("ERROR: " + cause);
            }
          }
        } else {
          generatedClass.writeToDestination(outputDir);
        }
      }
      return new ArrayList<>(generatedAlready.keySet());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}