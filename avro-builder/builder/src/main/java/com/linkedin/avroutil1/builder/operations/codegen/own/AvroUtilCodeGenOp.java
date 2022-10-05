/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avroutil1.builder.BuilderConsts;
import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.codegen.vanilla.ClasspathSchemaSet;
import com.linkedin.avroutil1.codegen.SpecificRecordClassGenerator;
import com.linkedin.avroutil1.codegen.SpecificRecordGenerationConfig;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.parser.avsc.AvroParseContext;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.writer.avsc.AvscSchemaWriter;
import com.linkedin.avroutil1.writer.avsc.AvscWriterConfig;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import javax.tools.JavaFileObject;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * a code generation operation using the avro-codegen module of avro-util
 */
public class AvroUtilCodeGenOp implements Operation {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroUtilCodeGenOp.class);

  private final CodeGenOpConfig config;

  public AvroUtilCodeGenOp(CodeGenOpConfig config) {
    this.config = config;
  }

  @Override
  public void run(OperationContext opContext) throws Exception {
    //mkdir any output folders that don't exist
    if (!config.getOutputSpecificRecordClassesRoot().exists() && !config.getOutputSpecificRecordClassesRoot()
        .mkdirs()) {
      throw new IllegalStateException(
          "unable to create destination folder " + config.getOutputSpecificRecordClassesRoot());
    }

    if (!config.isAvro702Handling()) {
      LOGGER.warn("Avro-702 handling was disabled, however Avro-702 handling cannot be  disabled in AvroUtilCodeGenOp.");
    }

    Set<File> avscFiles = new HashSet<>();
    Set<File> nonImportableFiles = new HashSet<>();
    String[] extensions = new String[]{BuilderConsts.AVSC_EXTENSION};
    long scanStart = System.currentTimeMillis();

    for (File inputRoot : config.getInputRoots()) {
      avscFiles.addAll(FileUtils.listFiles(inputRoot, extensions, true));
    }

    if (config.getNonImportableSourceRoots() != null) {
      for (File nonImportableRoot : config.getNonImportableSourceRoots()) {
        nonImportableFiles.addAll(FileUtils.listFiles(nonImportableRoot, extensions, true));
      }
    }

    long scanEnd = System.currentTimeMillis();

    int numFiles = avscFiles.size() + nonImportableFiles.size();
    if (numFiles == 0) {
      LOGGER.warn("no input schema files were found under roots " + config.getInputRoots());
      return;
    }
    LOGGER.info("found " + numFiles + " avsc schema files in " + (scanEnd - scanStart) + " millis");

    AvroParseContext context = new AvroParseContext();
    Set<AvscParseResult> parsedFiles = new HashSet<>();

    //build a classpath SchemaSet if classpath (cp) lookup is turned on
    ClasspathSchemaSet cpLookup = null;
    if (config.isIncludeClasspath()) {
      cpLookup = new ClasspathSchemaSet();
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
            && cpLookup != null) {
          Schema referencedSchema = cpLookup.getByName(ref);
          if (referencedSchema != null) {
            AvscParseResult referencedParseResult =
                parser.parse(AvroCompatibilityHelper.toAvsc(referencedSchema, AvscGenerationConfig.CORRECT_PRETTY));
            context.add(referencedParseResult, true);
          } else {
            Schema inheritedReferencedSchema = cpLookup.getByName(inheritedRef);
            if (inheritedReferencedSchema != null) {
              AvscParseResult referencedParseResult = parser.parse(
                  AvroCompatibilityHelper.toAvsc(inheritedReferencedSchema, AvscGenerationConfig.CORRECT_PRETTY));
              context.add(referencedParseResult, true);
            }
          }
        }
      }
    }
    // TODO: check and throw if schemas defined in the filesystem (parsedFiles) have duplicates on the classpath.

    //resolve any references across files that are part of this op (anything left would be external)
    context.resolveReferences();

    long parseEnd = System.currentTimeMillis();

    Map<String, AvscParseResult> allNamedSchemas = new HashMap<>();
    for (AvscParseResult parseResult : parsedFiles) {
      AvroSchema schema = parseResult.getTopLevelSchema();
      if (schema instanceof AvroNamedSchema) {
        String name = ((AvroNamedSchema) schema).getFullName();
        allNamedSchemas.put(name, parseResult);
      }
    }

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
            String baseSchema = null;
            AvscParseResult baseSchemaResult = null;
            for (AvscParseResult duplicateParseResult : duplicateEntry.getValue()) {
              if (baseSchema == null) {
                baseSchema = new AvscSchemaWriter().generateAvsc(duplicateParseResult.getDefinedSchema(fqcn),
                    AvscWriterConfig.CORRECT_MITIGATED);
                baseSchemaResult = duplicateParseResult;
                continue;
              }
              String currSchema = new AvscSchemaWriter().generateAvsc(duplicateParseResult.getDefinedSchema(fqcn),
                  AvscWriterConfig.CORRECT_MITIGATED);

              // TODO: compare canonical forms when canonicalization work is complete
              long baseLineNumber = baseSchemaResult.getDefinedSchema(fqcn).getCodeLocation().getEnd().getLineNumber();
              long duplicateLineNumber =
                  duplicateParseResult.getDefinedSchema(fqcn).getCodeLocation().getEnd().getLineNumber();
              String errorMsg = "schema " + fqcn + " found DIFFERENT in 2+ places: " + baseSchemaResult.getURI() + "#L"
                  + baseLineNumber + " and " + duplicateParseResult.getURI() + "#L" + duplicateLineNumber;
              if (!baseSchema.equals(currSchema)) {
                throw new RuntimeException("ERROR: " + errorMsg);
              } else {
                System.err.println("WARNING: " + errorMsg);
              }
            }
            break;
          default:
            throw new IllegalStateException("unhandled: " + config.getDupBehaviour());
        }
      }
    }

    LOGGER.info("parsed {} named schemas in {} millis, {} of which have duplicates", allNamedSchemas.size(),
        parseEnd - scanEnd, duplicates.size());

    //TODO fail if any errors or dups (depending on config) are found
    if (context.hasExternalReferences()) {
      //TODO - better formatting
      throw new UnsupportedOperationException(
          "unresolved referenced to external schemas: " + context.getExternalReferences());
    }

    List<Map<String, AvscParseResult>> namedSchemaChunks = new ArrayList<>();
    List<Map.Entry<String, AvscParseResult>> namedSchemaEntryList = new ArrayList(allNamedSchemas.entrySet());
    int schemaSize = allNamedSchemas.size();
    int numberOfChunks = 5;

    if (schemaSize > 500) {
      for (int chunkNumber = 0; chunkNumber < numberOfChunks; chunkNumber++) {
        Map<String, AvscParseResult> temp = new HashMap<>();
        for (int itemNumber = (chunkNumber) * schemaSize / (numberOfChunks);
            itemNumber < (chunkNumber + 1) * schemaSize / (numberOfChunks); itemNumber++) {
          temp.put(namedSchemaEntryList.get(itemNumber).getKey(), namedSchemaEntryList.get(itemNumber).getValue());
        }
        namedSchemaChunks.add(temp);
      }
    } else {
      namedSchemaChunks.add(allNamedSchemas);
    }


    for(Map<String, AvscParseResult> namedSchemaChunk : namedSchemaChunks) {
      SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
      HashSet<JavaFileObject> specificRecords = new HashSet<>(namedSchemaChunk.size());

      long genStart = System.currentTimeMillis();
      long errorCount = 0;
      for (Map.Entry<String, AvscParseResult> namedSchemaEntry : namedSchemaChunk.entrySet()) {
        String fullname = namedSchemaEntry.getKey();
        AvscParseResult fileParseResult = namedSchemaEntry.getValue();
        AvroNamedSchema namedSchema = fileParseResult.getDefinedSchema(fullname);

        try {
          List<JavaFileObject> javaFileObjects = generator.generateSpecificClassWithInternalTypes(namedSchema,
              SpecificRecordGenerationConfig.getBroadCompatibilitySpecificRecordGenerationConfig(config.getMinAvroVersion()));
          for (JavaFileObject fileObject : javaFileObjects) {
            if (!specificRecords.contains(fileObject)) {
              specificRecords.add(fileObject);
            }
          }
        } catch (Exception e) {
          errorCount++;
          //TODO - error-out
          LOGGER.error("failed to generate class for " + fullname + " defined in " + fileParseResult.getContext().getUri(), e);
        }
      }
      long genEnd = System.currentTimeMillis();

      if (errorCount > 0) {
        LOGGER.info("failed to generate {} java source files ({} generated successfully) in {} millis", errorCount,
            specificRecords.size(), genEnd - genStart);
      } else {
        LOGGER.info("generated {} java source files in {} millis", specificRecords.size(), genEnd - genStart);
      }

      writeJavaFilesToDisk(specificRecords, config.getOutputSpecificRecordClassesRoot());

      long writeEnd = System.currentTimeMillis();
      LOGGER.info("wrote out {} generated java source files under {} in {} millis", specificRecords.size(), config.getOutputSpecificRecordClassesRoot(), writeEnd - genEnd);
    }
    Set<File> allAvroFiles = new HashSet<>(avscFiles);
    allAvroFiles.addAll(nonImportableFiles);
    Set<AvroSchema> allTopLevelSchemas = new HashSet<>();
    for (AvscParseResult parsedFile : parsedFiles) {
      allTopLevelSchemas.add(parsedFile.getTopLevelSchema());
    }
    opContext.addParsedSchemas(allTopLevelSchemas, allAvroFiles);
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
      // We skipp adding the referenced parse results to `parsedFiles`
      parsedFiles.add(fileParseResult);
    }
    return parsedFiles;
  }

  private void writeJavaFilesToDisk(Collection<JavaFileObject> javaClassFiles, File outputFolder) {
    //make sure the output folder exists
    if (!outputFolder.exists() && !outputFolder.mkdirs()) {
      throw new IllegalStateException("unable to create output folder " + outputFolder);
    }

    //write out the files we generated
    for (JavaFileObject javaClass : javaClassFiles) {
      File outputFile = new File(outputFolder, javaClass.getName());

      //TODO - handle case where file already exists and make behaviour configurable (overwite, ignore, ignore_if_identical, etc)

      File parentFolder = outputFile.getParentFile();
      if (!parentFolder.exists() && !parentFolder.mkdirs()) {
        throw new IllegalStateException("unable to create output folder " + outputFolder);
      }

      try (
          FileOutputStream fos = new FileOutputStream(outputFile, false);
          OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
          Reader reader = javaClass.openReader(true)
      ) {
        IOUtils.copy(reader, writer);
        writer.flush();
        fos.flush();
      } catch (Exception e) {
        throw new IllegalStateException("while writing file " + outputFile, e);
      }
    }
  }
}
