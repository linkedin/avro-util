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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
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
    // Make sure the output folder exists
    File outputFolder = config.getOutputSpecificRecordClassesRoot();
    if (!outputFolder.exists() && !outputFolder.mkdirs()) {
      throw new IllegalStateException("unable to create output folder " + outputFolder);
    }
    final Path outputDirectoryPath = outputFolder.toPath();

    // Collect all named schemas.
    long collectStart = System.currentTimeMillis();
    Collection<AvroNamedSchema> allNamedSchemas = opContext.getAvroSchemas()
        .stream()
        .flatMap(schema -> {
          if (schema instanceof AvroNamedSchema) {
            return Stream.of((AvroNamedSchema) schema);
          } else if (AvroType.UNION.equals(schema.type())) {
            return ((AvroUnionSchema) schema).getTypes()
                .stream()
                .filter(schemaOrRef -> schemaOrRef.getSchema() instanceof AvroNamedSchema)
                .map(schemaOrRef -> (AvroNamedSchema) schemaOrRef.getSchema());
          } else {
            return Stream.empty();
          }
        })
        .flatMap(namedSchema -> {
          // Collect inner schemas.
          return Stream.concat(Stream.of(namedSchema),
              SpecificRecordGeneratorUtil.getNestedInternalSchemaList(namedSchema).stream());
        })
        .filter(namedSchema -> {
          // Skip codegen if schema is on classpath and config says to skip
          return !config.shouldSkipCodegenIfSchemaOnClasspath() || !doesSchemaExistOnClasspath(namedSchema,
              opContext.getLookupSchemaSet());
        })
        .collect(Collectors.toMap(AvroNamedSchema::getFullName, Function.identity(), (s1, s2) -> s1))
        .values();
    long collectEnd = System.currentTimeMillis();
    LOGGER.info("Collected {} named avro schemas in {} millis", allNamedSchemas.size(), collectEnd - collectStart);

    // Generate avro binding java classes.
    long genStart = System.currentTimeMillis();
    final SpecificRecordGenerationConfig generationConfig =
        SpecificRecordGenerationConfig.getBroadCompatibilitySpecificRecordGenerationConfig(
            AvroJavaStringRepresentation.fromJson(config.getStringRepresentation().toString()),
            AvroJavaStringRepresentation.fromJson(config.getMethodStringRepresentation().toString()),
            config.getMinAvroVersion(), config.isUtf8EncodingPutByIndexEnabled());
    final SpecificRecordClassGenerator generator = new SpecificRecordClassGenerator();
    List<JavaFile> generatedClasses = allNamedSchemas.parallelStream().map(namedSchema -> {
      try {
        // Top level schema
        return generator.generateSpecificClass(namedSchema, generationConfig);
      } catch (Exception e) {
        throw new RuntimeException("failed to generate class for " + namedSchema.getFullName(), e);
      }
    }).collect(Collectors.toList());
    long genEnd = System.currentTimeMillis();
    LOGGER.info("Generated {} java source files in {} millis", generatedClasses.size(), genEnd - genStart);

    // Write to disk.
    writeJavaFilesToDisk(generatedClasses, outputDirectoryPath);
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
    LOGGER.info("Wrote out {} generated java source files under {} in {} millis", filesWritten, outputFolderPath,
        writeEnd - writeStart);
  }
}
