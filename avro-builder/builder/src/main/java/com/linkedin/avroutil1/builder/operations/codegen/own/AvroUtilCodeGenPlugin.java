/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.own;

import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.operations.SchemaSet;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.plugins.BuilderPlugin;
import com.linkedin.avroutil1.builder.plugins.BuilderPluginContext;
import com.linkedin.avroutil1.codegen.SpecificRecordClassGenerator;
import com.linkedin.avroutil1.codegen.SpecificRecordGenerationConfig;
import com.linkedin.avroutil1.codegen.SpecificRecordGeneratorUtil;
import com.linkedin.avroutil1.model.AvroJavaStringRepresentation;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.squareup.javapoet.JavaFile;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A code generation plugin using the avro-codegen module of avro-util
 */
public class AvroUtilCodeGenPlugin implements BuilderPlugin {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroUtilCodeGenPlugin.class);

  private final CodeGenOpConfig config;

  public AvroUtilCodeGenPlugin(CodeGenOpConfig config) {
    this.config = config;
  }

  @Override
  public Set<Integer> supportedApiVersions() {
    return Collections.singleton(1);
  }

  @Override
  public void createOperations(BuilderPluginContext context) {
    context.add(this::generateCode);
  }

  private void generateCode(OperationContext opContext) {
    //mkdir any output folders that don't exist
    if (!config.getOutputSpecificRecordClassesRoot().exists() && !config.getOutputSpecificRecordClassesRoot()
        .mkdirs()) {
      throw new IllegalStateException(
          "unable to create destination folder " + config.getOutputSpecificRecordClassesRoot());
    }

    final AtomicInteger schemaCounter = new AtomicInteger(0);
    final int schemaChunkSize = 500;
    Collection<List<AvroNamedSchema>> allNamedSchemaList = opContext.getAvroSchemas().stream().flatMap(schema -> {
      if (schema instanceof AvroNamedSchema) {
        return Stream.of((AvroNamedSchema) schema);
      } else if (AvroType.UNION.equals(schema.type())) {
        return ((AvroUnionSchema) schema).getTypes()
            .stream()
            .map(schemaOrRef -> (AvroNamedSchema) schemaOrRef.getSchema());
      } else {
        return Stream.empty();
      }
    }).collect(Collectors.groupingBy(it -> schemaCounter.getAndIncrement() / schemaChunkSize)).values();

    long genStart = System.currentTimeMillis();

    final SpecificRecordGenerationConfig generationConfig =
        SpecificRecordGenerationConfig.getBroadCompatibilitySpecificRecordGenerationConfig(
            AvroJavaStringRepresentation.fromJson(config.getStringRepresentation().toString()),
            AvroJavaStringRepresentation.fromJson(config.getMethodStringRepresentation().toString()),
            config.getMinAvroVersion(), config.isUtf8EncodingPutByIndexEnabled());

    // Make sure the output folder exists
    File outputFolder = config.getOutputSpecificRecordClassesRoot();
    if (!outputFolder.exists() && !outputFolder.mkdirs()) {
      throw new IllegalStateException("unable to create output folder " + outputFolder);
    }
    final Path outputDirectoryPath = outputFolder.toPath();
    final SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();

    int totalGeneratedClasses = allNamedSchemaList.parallelStream().map(allNamedSchemas -> {
      HashSet<String> alreadyGeneratedSchemaNames = new HashSet<>();
      List<JavaFile> generatedSpecificClasses = new ArrayList<>(allNamedSchemas.size());
      for (AvroNamedSchema namedSchema : allNamedSchemas) {
        try {
          if (!alreadyGeneratedSchemaNames.contains(namedSchema.getFullName())) {
            // skip codegen if schema is on classpath and config says to skip
            if (config.shouldSkipCodegenIfSchemaOnClasspath() &&
                doesSchemaExistOnClasspath(namedSchema, opContext.getLookupSchemaSet())) {
              continue;
            }

            //top level schema
            alreadyGeneratedSchemaNames.add(namedSchema.getFullName());
            generatedSpecificClasses.add(generator.generateSpecificClass(namedSchema, generationConfig));

            // generate internal schemas if not already present
            List<AvroNamedSchema> internalSchemaList =
                SpecificRecordGeneratorUtil.getNestedInternalSchemaList(namedSchema);
            for (AvroNamedSchema namedInternalSchema : internalSchemaList) {
              if (!alreadyGeneratedSchemaNames.contains(namedInternalSchema.getFullName())) {
                // skip codegen for nested schemas if schema is on classpath and config says to skip
                if (config.shouldSkipCodegenIfSchemaOnClasspath() &&
                    doesSchemaExistOnClasspath(namedInternalSchema, opContext.getLookupSchemaSet())) {
                  continue;
                }

                generatedSpecificClasses.add(generator.generateSpecificClass(namedInternalSchema, generationConfig));
                alreadyGeneratedSchemaNames.add(namedInternalSchema.getFullName());
              }
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("failed to generate class for " + namedSchema.getFullName(), e);
        }
      }
      writeJavaFilesToDisk(generatedSpecificClasses, outputDirectoryPath);
      return generatedSpecificClasses.size();
    }).reduce(0, Integer::sum);

    long genEnd = System.currentTimeMillis();
    LOGGER.info("Generated {} java source files in {} millis", totalGeneratedClasses, genEnd - genStart);
  }

  private boolean doesSchemaExistOnClasspath(AvroNamedSchema schema, SchemaSet schemaSet) {
    if (schemaSet == null) {
      return false;
    }

    return schemaSet.getByName(schema.getFullName()) != null;
  }

  private void writeJavaFilesToDisk(Collection<JavaFile> javaFiles, Path outputFolderPath) {

    long writeStart = System.currentTimeMillis();

    // write out the files we generated
    int filesWritten = javaFiles.parallelStream().map(javaFile -> {
      try {
        javaFile.writeToPath(outputFolderPath);
      } catch (Exception e) {
        throw new IllegalStateException("while writing file " + javaFile.typeSpec.name, e);
      }

      return 1;
    }).reduce(0, Integer::sum);

    long writeEnd = System.currentTimeMillis();
    LOGGER.info("wrote out {} generated java source files under {} in {} millis", filesWritten, outputFolderPath,
        writeEnd - writeStart);
  }
}
