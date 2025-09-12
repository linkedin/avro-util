/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.vanilla;

import com.linkedin.avroutil1.builder.DuplicateSchemaBehaviour;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import com.linkedin.avroutil1.compatibility.ConfigurableSchemaComparator;
import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;

public class SpecificCompilerHelper {

  private SpecificCompilerHelper() { }

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


  /**
   * Deduplicates schemas by full name, handling duplicates according to the specified behavior.
   * @param schemas List of schemas to deduplicate
   * @param onDups Behavior for handling duplicate schemas
   * @param ignoreDups Set of schema full names to ignore for duplicates
   * @return List of unique schemas
   */
  private static List<Schema> deduplicateSchemas(
      List<Schema> schemas,
      DuplicateSchemaBehaviour onDups,
      Collection<String> ignoreDups,
      Set<String> jsonPropsToIgnore
  ) {
    if (schemas == null || schemas.isEmpty()) {
      return Collections.emptyList();
    }

    Map<String, Schema> uniqueSchemas = new HashMap<>();
    List<Schema> result = new ArrayList<>();
    
    for (Schema schema : schemas) {
      if (schema == null) {
        continue;
      }
      
      String fullName = schema.getFullName();
      if (fullName == null) {
        // Skip schemas without a full name
        continue;
      }
      
      Schema existing = uniqueSchemas.get(fullName);
      if (existing == null) {
        // First time seeing this schema name
        uniqueSchemas.put(fullName, schema);
        result.add(schema);
        continue;
      }
      
      // Found a duplicate schema name - check if they're equivalent
      // Use strict comparison without ignoring any properties to match the original behavior
      // which compared the generated Java code (including all schema properties in SCHEMA$)
      // Configure schema comparison with optional property ignores
      SchemaComparisonConfiguration comparisonConfig = SchemaComparisonConfiguration.STRICT;
      if (jsonPropsToIgnore != null && !jsonPropsToIgnore.isEmpty()) {
        comparisonConfig = comparisonConfig.jsonPropNamesToIgnore(jsonPropsToIgnore);
      }
      
      boolean equal = ConfigurableSchemaComparator.equals(
          schema,
          existing,
          comparisonConfig
      );

      if (equal) {
        // Identical schemas - use the same message format as the original
        String cause = "avro type " + fullName + " defined (identically) multiple times";
        if (onDups == DuplicateSchemaBehaviour.FAIL && (ignoreDups == null || !ignoreDups.contains(fullName))) {
          throw new IllegalStateException("ERROR: " + cause);
        } else if (onDups != DuplicateSchemaBehaviour.FAIL) {
          System.err.println("WARNING: " + cause);
        }
        // Keep the first schema and skip the duplicate
      } else {
        // Different schemas with same name - format matches original but uses schema strings instead of file contents
        String cause = String.format(
            "avro type %s defined DIFFERENTLY multiple times: %s vs %s",
            fullName,
            String.valueOf(existing),
            String.valueOf(schema)
        );
        if (onDups == DuplicateSchemaBehaviour.WARN) {
          System.err.println("WARNING: " + cause);
        } else if (ignoreDups == null || !ignoreDups.contains(fullName)) {
          throw new IllegalStateException("ERROR: " + cause);
        }
        // Keep the first schema and skip the conflicting one
      }
    }
    
    return result;
  }

  /**
   * Compiles Avro schemas to Java classes with configurable schema comparison options.
   * @param schemas List of schemas to compile
   * @param outputDir Output directory for generated classes
   * @param minTargetVersion Minimum Avro version to target
   * @param toSkip Fully qualified class names to skip during generation
   * @param ignoreDups Set of schema full names to ignore for duplicates
   * @param onDups Behavior for handling duplicate schemas
   * @param codeGenerationConfig Configuration for code generation
   * @param jsonPropsToIgnore Set of JSON property names to ignore during schema comparison
   * @return List of generated files
   */
  public static List<File> compile(
      List<Schema> schemas,
      File outputDir,
      AvroVersion minTargetVersion,
      Collection<String> toSkip,
      Collection<String> ignoreDups,
      DuplicateSchemaBehaviour onDups,
      CodeGenerationConfig codeGenerationConfig,
      Set<String> jsonPropsToIgnore
  ) {
    return compileImpl(
        schemas,
        outputDir,
        minTargetVersion,
        toSkip,
        ignoreDups,
        onDups,
        codeGenerationConfig,
        jsonPropsToIgnore != null ? jsonPropsToIgnore : Collections.emptySet()
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
    return compileImpl(
        schemas,
        outputDir,
        minTargetVersion,
        toSkip,
        ignoreDups,
        onDups,
        codeGenerationConfig,
        Collections.emptySet()
    );
  }

  private static List<File> compileImpl(
      List<Schema> schemas,
      File outputDir,
      AvroVersion minTargetVersion,
      Collection<String> toSkip,
      Collection<String> ignoreDups,
      DuplicateSchemaBehaviour onDups,
      CodeGenerationConfig codeGenerationConfig,
      Set<String> jsonPropsToIgnore
  ) {
    if (outputDir == null) {
      throw new IllegalArgumentException("must provide output root dir");
    }
    if (schemas == null || schemas.isEmpty()) {
      return Collections.emptyList();
    }
    
    // First deduplicate schemas at the schema level
    List<Schema> uniqueSchemas = deduplicateSchemas(schemas, onDups, ignoreDups, jsonPropsToIgnore);
    
    // Generate code only for unique schemas
    Collection<AvroGeneratedSourceCode> generatedClasses = AvroCompatibilityHelper.compile(
        uniqueSchemas, minTargetVersion, AvroVersion.latest(), codeGenerationConfig
    );
    
    try {
      Map<File, AvroGeneratedSourceCode> generatedAlready = new HashMap<>(generatedClasses.size());
      List<File> generatedFiles = new ArrayList<>();
      
      for (AvroGeneratedSourceCode generatedClass : generatedClasses) {
        String fqcn = generatedClass.getFullyQualifiedClassName();
        if (toSkip != null && toSkip.contains(fqcn)) {
          continue;
        }
        
        File outputFile = new File(outputDir, generatedClass.getPath()).getCanonicalFile();
        
        // This should never happen since we deduplicated schemas, but just in case
        if (generatedAlready.put(outputFile, generatedClass) == null) {
          // No conflict, write the file
          generatedClass.writeToDestination(outputDir);
          generatedFiles.add(outputFile);
        }
      }
      
      return generatedFiles;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to generate code", e);
    }
  }
}