/*
 * Copyright 2024 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.operations.SchemaSet;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.codegen.OperationContextBuilder;
import com.linkedin.avroutil1.builder.operations.codegen.util.AvscFileFinderUtil;
import com.linkedin.avroutil1.builder.operations.codegen.vanilla.ClasspathSchemaSet;
import com.linkedin.avroutil1.builder.operations.codegen.vanilla.ResolverPathSchemaSet;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.parser.avsc.AvroParseContext;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.util.ConfigurableAvroSchemaComparator;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class AvroUtilOperationContextBuilder implements OperationContextBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroUtilOperationContextBuilder.class);

  @Override
  public OperationContext buildOperationContext(CodeGenOpConfig config) throws Exception {
    if (!config.isAvro702Handling()) {
      LOGGER.warn("Avro-702 handling was disabled, however Avro-702 handling cannot be disabled.");
    }

    Set<File> avscFiles = new HashSet<>();
    Set<File> nonImportableFiles = new HashSet<>();
    long scanStart = System.currentTimeMillis();

    for (File inputRoot : config.getInputRoots()) {
      avscFiles.addAll(AvscFileFinderUtil.findFiles(inputRoot));
    }

    if (config.getNonImportableSourceRoots() != null) {
      for (File nonImportableRoot : config.getNonImportableSourceRoots()) {
        nonImportableFiles.addAll(AvscFileFinderUtil.findFiles(nonImportableRoot));
      }
    }

    long scanEnd = System.currentTimeMillis();

    int numFiles = avscFiles.size() + nonImportableFiles.size();
    if (numFiles == 0) {
      LOGGER.warn("no input schema files were found under roots " + config.getInputRoots());
      return new OperationContext(Collections.emptySet(), Collections.emptySet(), null);
    }
    LOGGER.info("found " + numFiles + " avsc schema files in " + (scanEnd - scanStart) + " millis");

    AvroParseContext context = new AvroParseContext();
    Set<AvscParseResult> parsedFiles = new HashSet<>();

    final SchemaSet lookupSchemaSet;
    if (config.getResolverPath() != null) {
      //build a resolver path SchemaSet if ResolverPath is set
      lookupSchemaSet = new ResolverPathSchemaSet(config.getResolverPath());
    } else if (config.isIncludeClasspath()) {
      //build a classpath SchemaSet if classpath (cp) lookup is turned on
      lookupSchemaSet = new ClasspathSchemaSet();
    } else {
      lookupSchemaSet = null;
    }
    parsedFiles.addAll(parseAvscFiles(avscFiles, true, context));
    parsedFiles.addAll(parseAvscFiles(nonImportableFiles, false, context));

    // Lookup unresolved schemas in classpath
    for (AvscParseResult parsedFile : parsedFiles) {
      for (SchemaOrRef externalReference : parsedFile.getExternalReferences()) {
        AvscParser parser = new AvscParser();
        String ref = externalReference.getRef();
        String inheritedRef = externalReference.getInheritedName();
        if (!context.getAllNamedSchemas().containsKey(inheritedRef) && !context.getAllNamedSchemas().containsKey(ref)
            && lookupSchemaSet != null) {
          Schema referencedSchema = lookupSchemaSet.getByName(ref);
          if (referencedSchema != null) {
            AvscParseResult referencedParseResult =
                parser.parse(AvroCompatibilityHelper.toAvsc(referencedSchema, AvscGenerationConfig.CORRECT_PRETTY));
            context.add(referencedParseResult, true);
          } else {
            Schema inheritedReferencedSchema = lookupSchemaSet.getByName(inheritedRef);
            if (inheritedReferencedSchema != null) {
              AvscParseResult referencedParseResult = parser.parse(
                  AvroCompatibilityHelper.toAvsc(inheritedReferencedSchema, AvscGenerationConfig.CORRECT_PRETTY));
              context.add(referencedParseResult, true);
            }
          }
        }
      }
    }
    // check and throw if schemas defined in the filesystem (parsedFiles) are not equal if also defined on the classpath.
    if (lookupSchemaSet != null) {
      for (AvscParseResult parsedFile : parsedFiles) {
        for (Map.Entry<String, AvroNamedSchema> entrySet : parsedFile.getDefinedNamedSchemas().entrySet()) {
          AvroNamedSchema schema = entrySet.getValue();
          String fullName = entrySet.getKey();
          Schema cpSchema = lookupSchemaSet.getByName(fullName);
          if (cpSchema != null) {
            // check if the schema on classpath is the same as the one we are trying to generate
            AvroSchema avroSchemaFromClasspath = (new AvscParser()).parse(cpSchema.toString()).getTopLevelSchema();
            SchemaComparisonConfiguration comparisonConfig = SchemaComparisonConfiguration.STRICT;
            if (config.getJsonPropsToIgnore() != null && !config.getJsonPropsToIgnore().isEmpty()) {
              comparisonConfig = comparisonConfig.jsonPropNamesToIgnore(config.getJsonPropsToIgnore());
            }
            boolean areEqual = ConfigurableAvroSchemaComparator.equals(avroSchemaFromClasspath, schema, comparisonConfig);
            if (!areEqual) {
              throw new IllegalStateException("Schema with name " + fullName
                  + " is defined in the filesystem and on the classpath, but the two schemas are not equal.");
            }
          }
        }
      }
    }

    //resolve any references across files that are part of this op (anything left would be external)
    context.resolveReferences();

    long parseEnd = System.currentTimeMillis();

    // Handle duplicate schemas
    Map<String, List<AvscParseResult>> duplicates = context.getDuplicates();
    for (Map.Entry<String, List<AvscParseResult>> duplicateEntry : duplicates.entrySet()) {
      String fqcn = duplicateEntry.getKey();
      StringJoiner allFilesString = new StringJoiner(",", "", ".");
      for (AvscParseResult avscParseResult : duplicateEntry.getValue()) {
        long lineNumber = avscParseResult.getDefinedSchema(fqcn).getCodeLocation().getStart().getLineNumber();
        allFilesString.add(avscParseResult.getURI().toString() + "#L" + lineNumber);
      }
      if (!config.getDuplicateSchemasToIgnore().contains(fqcn)) {
        switch (config.getDupBehaviour()) {
          case FAIL:
            throw new RuntimeException("ERROR: schema " + fqcn + " found in 2+ places: " + allFilesString);
          case WARN:
            System.err.println("WARNING: schema " + fqcn + " found in 2+ places: " + allFilesString);
            break;
          case FAIL_IF_DIFFERENT:
            AvroNamedSchema baseNamed = null;
            AvscParseResult baseSchemaResult = null;
            SchemaComparisonConfiguration comparisonConfig = SchemaComparisonConfiguration.STRICT;
            if (config.getJsonPropsToIgnore() != null && !config.getJsonPropsToIgnore().isEmpty()) {
              comparisonConfig = comparisonConfig.jsonPropNamesToIgnore(config.getJsonPropsToIgnore());
            }
            for (AvscParseResult duplicateParseResult : duplicateEntry.getValue()) {
              AvroNamedSchema currentNamed = duplicateParseResult.getDefinedSchema(fqcn);
              if (baseNamed == null) {
                baseNamed = currentNamed;
                baseSchemaResult = duplicateParseResult;
                continue;
              }
              // Compare using configurable comparator on Avro model with optional json-prop ignores
              boolean equal = com.linkedin.avroutil1.util.ConfigurableAvroSchemaComparator.equals(baseNamed, currentNamed, comparisonConfig);
              long baseLineNumber = baseSchemaResult.getDefinedSchema(fqcn).getCodeLocation().getEnd().getLineNumber();
              long duplicateLineNumber = currentNamed.getCodeLocation().getEnd().getLineNumber();
              String msg = "schema " + fqcn + " found DIFFERENT in 2+ places: " + baseSchemaResult.getURI() + "#L"
                  + baseLineNumber + " and " + duplicateParseResult.getURI() + "#L" + duplicateLineNumber;
              if (!equal) {
                throw new RuntimeException("ERROR: " + msg);
              } else {
                System.err.println("WARNING: " + msg);
              }
            }
            break;
          default:
            throw new IllegalStateException("unhandled: " + config.getDupBehaviour());
        }
      }
    }

    //TODO fail if any errors or dups (depending on config) are found
    if (context.hasExternalReferences()) {
      //TODO - better formatting
      throw new UnsupportedOperationException(
          "unresolved referenced to external schemas: " + context.getExternalReferences());
    }

    LOGGER.info("Parsed {} avsc files in {} millis, {} of which have duplicates", parsedFiles.size(),
        parseEnd - scanEnd, duplicates.size());

    long contextStart = System.currentTimeMillis();
    Set<File> allAvroFiles = Stream.concat(avscFiles.stream(), nonImportableFiles.stream()).collect(Collectors.toSet());
    Set<AvroSchema> allTopLevelSchemas =
        parsedFiles.stream().map(AvscParseResult::getTopLevelSchema).collect(Collectors.toSet());
    OperationContext operationContext = new OperationContext(allTopLevelSchemas, allAvroFiles, lookupSchemaSet);
    long contextEnd = System.currentTimeMillis();
    LOGGER.info("Added {} top-level schemas across {} files to context in {} millis", allTopLevelSchemas.size(),
        allAvroFiles.size(), contextEnd - contextStart);

    return operationContext;
  }

  /**
   * A helper to parse avsc files.
   *
   * @param avscFiles Avsc files to parse
   * @param areFilesImportable whether to allow other avsc files to import from this avsc file
   * @param context the full parsing context for this "run".
   * @return a set of all the parsed file results (a Set of AvscParseResult)
   */
  private Set<AvscParseResult> parseAvscFiles(Set<File> avscFiles, boolean areFilesImportable, AvroParseContext context) {
    AvscParser parser = new AvscParser();
    HashSet<AvscParseResult> parsedFiles = new HashSet<>();
    for (File p : avscFiles) {
      AvscParseResult fileParseResult = parser.parse(p);
      Throwable parseError = fileParseResult.getParseError();
      if (parseError != null) {
        throw new IllegalArgumentException("failed to parse file " + p.getAbsolutePath(), parseError);
      }

      context.add(fileParseResult, areFilesImportable);
      // We skip adding the referenced parse results to `parsedFiles`
      parsedFiles.add(fileParseResult);
    }
    return parsedFiles;
  }
}
