/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.codegen.util.AvscFileFinderUtil;
import com.linkedin.avroutil1.builder.operations.codegen.vanilla.ClasspathSchemaSet;
import com.linkedin.avroutil1.codegen.SpecificRecordClassGenerator;
import com.linkedin.avroutil1.codegen.SpecificRecordGenerationConfig;
import com.linkedin.avroutil1.codegen.SpecificRecordGeneratorUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.SchemaComparisonConfiguration;
import com.linkedin.avroutil1.model.AvroJavaStringRepresentation;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.parser.avsc.AvroParseContext;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import com.linkedin.avroutil1.util.ConfigurableAvroSchemaComparator;
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
    // check and throw if schemas defined in the filesystem (parsedFiles) are not equal if also defined on the classpath.
    if (cpLookup != null) {
      for (AvscParseResult parsedFile : parsedFiles) {
        for (Map.Entry<String, AvroNamedSchema> entrySet : parsedFile.getDefinedNamedSchemas().entrySet()) {
          AvroNamedSchema schema = entrySet.getValue();
          String fullName = entrySet.getKey();
          Schema cpSchema = cpLookup.getByName(fullName);
          if (cpSchema != null) {
            // check if the schema on classpath is the same as the one we are trying to generate
            AvroSchema avroSchemaFromClasspath = (new AvscParser()).parse(cpSchema.toString()).getTopLevelSchema();
            boolean areEqual = ConfigurableAvroSchemaComparator.equals(avroSchemaFromClasspath, schema,
                SchemaComparisonConfiguration.STRICT);
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

    int schemaChunkSize = 500, schemaCounter = 0, totalSchemaParsed = 0;

    List<Map<String, AvroNamedSchema>> allNamedSchemaList = new ArrayList<>();
    Map<String, AvroNamedSchema> namedSchemaChunk = new HashMap<>();
    for (AvscParseResult parseResult : parsedFiles) {
      if(schemaCounter >= schemaChunkSize) {
        allNamedSchemaList.add(namedSchemaChunk);
        namedSchemaChunk = new HashMap<>();
        totalSchemaParsed += schemaCounter;
        schemaCounter = 0;
      }
      AvroSchema schema = parseResult.getTopLevelSchema();
      if (schema instanceof AvroNamedSchema) {
        String name = ((AvroNamedSchema) schema).getFullName();
        namedSchemaChunk.put(name, (AvroNamedSchema) parseResult.getTopLevelSchema());
      } else if (AvroType.UNION.equals(schema.type())) {
        for(SchemaOrRef schemaOrRef : ((AvroUnionSchema) schema).getTypes()) {
          String name = ((AvroNamedSchema) schemaOrRef.getSchema()).getFullName();
          namedSchemaChunk.put(name, (AvroNamedSchema) schemaOrRef.getSchema());
          schemaCounter++;
        }
      }
      schemaCounter++;
    }
    if(!namedSchemaChunk.isEmpty()) {
      totalSchemaParsed += schemaCounter;
      allNamedSchemaList.add(namedSchemaChunk);
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

    LOGGER.info("parsed {} named schemas in {} millis, {} of which have duplicates", totalSchemaParsed,
        parseEnd - scanEnd, duplicates.size());

    //TODO fail if any errors or dups (depending on config) are found
    if (context.hasExternalReferences()) {
      //TODO - better formatting
      throw new UnsupportedOperationException(
          "unresolved referenced to external schemas: " + context.getExternalReferences());
    }


    SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    HashSet<String> alreadyGeneratedSchemas = new HashSet<>();
    List<JavaFileObject> generatedSpecificClasses;
    int totalGeneratedClasses = 0;

    long genStart = System.currentTimeMillis();
    long errorCount = 0;
    List<AvroNamedSchema> internalSchemaList;

    for (Map<String, AvroNamedSchema> allNamedSchemas : allNamedSchemaList) {
      generatedSpecificClasses = new ArrayList<>(totalSchemaParsed);
      for (Map.Entry<String, AvroNamedSchema> namedSchemaEntry : allNamedSchemas.entrySet()) {
        String fullname = namedSchemaEntry.getKey();
        AvroNamedSchema namedSchema = namedSchemaEntry.getValue();

        try {

          if (!alreadyGeneratedSchemas.contains(namedSchema.getFullName())) {
            // skip codegen if schema is on classpath and config says to skip
            if (config.shouldSkipCodegenIfSchemaOnClasspath() && doesSchemaExistOnClasspath(namedSchema, cpLookup)) {
              continue;
            }

            //top level schema
            alreadyGeneratedSchemas.add(namedSchema.getFullName());
            generatedSpecificClasses.add(generator.generateSpecificClass(namedSchema,
                SpecificRecordGenerationConfig.getBroadCompatibilitySpecificRecordGenerationConfig(
                    AvroJavaStringRepresentation.fromJson(config.getStringRepresentation().toString()),
                    AvroJavaStringRepresentation.fromJson(config.getMethodStringRepresentation().toString()),
                    config.getMinAvroVersion(), config.isUtf8EncodingPutByIndexEnabled())));

            // generate internal schemas if not already present
            internalSchemaList = SpecificRecordGeneratorUtil.getNestedInternalSchemaList(namedSchema);
            for (AvroNamedSchema namedInternalSchema : internalSchemaList) {
              if (!alreadyGeneratedSchemas.contains(namedInternalSchema.getFullName())) {
                // skip codegen for nested schemas if schema is on classpath and config says to skip
                if (config.shouldSkipCodegenIfSchemaOnClasspath() && doesSchemaExistOnClasspath(namedInternalSchema,
                    cpLookup)) {
                  continue;
                }

                generatedSpecificClasses.add(generator.generateSpecificClass(namedInternalSchema,
                    SpecificRecordGenerationConfig.getBroadCompatibilitySpecificRecordGenerationConfig(
                        AvroJavaStringRepresentation.fromJson(config.getStringRepresentation().toString()),
                        AvroJavaStringRepresentation.fromJson(config.getMethodStringRepresentation().toString()),
                        config.getMinAvroVersion(), config.isUtf8EncodingPutByIndexEnabled())));
                alreadyGeneratedSchemas.add(namedInternalSchema.getFullName());
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("failed to generate class for " + fullname, e);
        }
      }
      writeJavaFilesToDisk(generatedSpecificClasses, config.getOutputSpecificRecordClassesRoot());
      totalGeneratedClasses += generatedSpecificClasses.size();
    }

    long genEnd = System.currentTimeMillis();

    if (errorCount > 0) {
      LOGGER.info("failed to generate {} java source files ({} generated successfully) in {} millis", errorCount,
          totalGeneratedClasses, genEnd - genStart);
    } else {
      LOGGER.info("generated {} java source files in {} millis", totalGeneratedClasses, genEnd - genStart);
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

  private boolean doesSchemaExistOnClasspath(AvroNamedSchema schema, ClasspathSchemaSet cpLookup) {
    if (cpLookup == null) {
      return false;
    }

    return cpLookup.getByName(schema.getFullName()) != null;
  }

  private void writeJavaFilesToDisk(Collection<JavaFileObject> javaClassFiles, File outputFolder) {

    long writeStart = System.currentTimeMillis();

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
    long writeEnd = System.currentTimeMillis();
    LOGGER.info("wrote out {} generated java source files under {} in {} millis", javaClassFiles.size(), outputFolder,
        writeEnd - writeStart);
  }
}
