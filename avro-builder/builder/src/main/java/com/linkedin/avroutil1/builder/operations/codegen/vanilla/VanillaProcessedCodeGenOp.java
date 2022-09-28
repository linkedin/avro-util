/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.vanilla;

import com.linkedin.avroutil1.builder.BuilderConsts;
import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.compatibility.Avro702Checker;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * a code generation operation using vanilla avro-compiler + helper post-processing
 */
public class VanillaProcessedCodeGenOp implements Operation {
  private static final Logger LOGGER = LoggerFactory.getLogger(VanillaProcessedCodeGenOp.class);

  private final CodeGenOpConfig config;

  public VanillaProcessedCodeGenOp(CodeGenOpConfig config) {
    this.config = config;
  }

  @Override
  public void run(OperationContext opContext) throws Exception {
    //mkdir any output folders that dont exist
    if (!config.getOutputSpecificRecordClassesRoot().exists() && !config.getOutputSpecificRecordClassesRoot().mkdirs()) {
      throw new IllegalStateException("unable to create destination folder " + config.getOutputSpecificRecordClassesRoot());
    }
    if (config.getOutputExpandedSchemasRoot() != null) {
      if (!config.getOutputExpandedSchemasRoot().exists() && !config.getOutputExpandedSchemasRoot().mkdirs()) {
        throw new IllegalStateException("unable to create destination folder " + config.getOutputExpandedSchemasRoot());
      }
    }

    AvscGenerationConfig avscConfig = AvscGenerationConfig.CORRECT_MITIGATED_ONELINE;
    if (!config.isAvro702Handling()) {
      LOGGER.warn("Avro-702 handling was disabled. It is HIGHLY recommended that you enable Avro-702 handling.");
    }

    //build a classpath SchemaSet if classpath (cp) lookup is turned on
    ClasspathSchemaSet cpLookup = null;
    if (config.isIncludeClasspath()) {
      cpLookup = new ClasspathSchemaSet();
    }

    FileSystemSchemaSetProvider provider = new FileSystemSchemaSetProvider(
        config.getInputRoots(),
        config.getNonImportableSourceRoots(),
        FileSystemSchemaSetProvider.DEFAULT_SCHEMA_SUFFIX,
        cpLookup
    );

    //load ALL the schemas (deeply-nested ones included) to allow discovery/import of inner schemas
    List<Schema> schemas = provider.loadSchemas(false).getAll();
    List<Schema> schemasFoundOnClasspath = cpLookup != null ? cpLookup.getAll() : Collections.emptyList();
    if (!schemasFoundOnClasspath.isEmpty()) {
      StringJoiner csv = new StringJoiner(", ");
      for (Schema onCP : schemasFoundOnClasspath) {
        csv.add(onCP.getFullName());
      }
      LOGGER.info("the following schemas were found on the classpath and will not be generated: " + csv);
      schemas.removeAll(schemasFoundOnClasspath);
    }

    Set<File> avroFiles = new HashSet<>();
    String[] extensions = new String[]{BuilderConsts.AVSC_EXTENSION};
    if (config.getNonImportableSourceRoots() != null) {
      for (File include : config.getNonImportableSourceRoots()) {
        avroFiles.addAll(FileUtils.listFiles(include, extensions, true));
      }
    }
    for (File file : config.getInputRoots()) {
      if (file.isDirectory()) {
        avroFiles.addAll(FileUtils.listFiles(file, extensions, true));
      } else {
        avroFiles.add(file);
      }
    }

    Map<String, String> schemaNameToFilepath = new HashMap<>();
    for (File avroFile : avroFiles) {
      String filePath = avroFile.getAbsolutePath();
      try (FileInputStream is = new FileInputStream(avroFile)) {
        String fileContents = IOUtils.toString(is, StandardCharsets.UTF_8);
        JSONTokener tokener = new JSONTokener(fileContents);
        JSONObject json;
        String fqcn;
        try {
          json = (JSONObject) tokener.nextValue();
          fqcn = fqcnFromSchema(json);
        } catch (Exception e) {
          throw new IllegalStateException("caught exception parsing " + filePath, e);
        }
        if (schemaNameToFilepath.containsKey(fqcn) && !config.getDuplicateSchemasToIgnore().contains(fqcn)) {
          String otherPath = schemaNameToFilepath.get(fqcn);
          switch (config.getDupBehaviour()) {
            case FAIL:
              throw new RuntimeException("ERROR: schema " + fqcn + " found in 2+ places: " + otherPath + " and " + filePath);
            case WARN:
              System.err.println("WARNING: schema " + fqcn + " found in 2+ places: " + otherPath + " and " + filePath);
              break;
            case FAIL_IF_DIFFERENT:
              String otherContents = readFile(otherPath);
              boolean identical = fileContents.equals(otherContents);
              if (!identical) {
                throw new RuntimeException("ERROR: schema " + fqcn + " found DIFFERENT in 2+ places: " + otherPath + " and " + filePath);
              } else {
                System.err.println("WARNING: schema " + fqcn + " found identical in 2+ places: " + otherPath + " and " + filePath);
              }
              break;
            default:
              throw new IllegalStateException("unhandled: " + config.getDupBehaviour());
          }
        }
        schemaNameToFilepath.put(fqcn, filePath);
      }
    }

    LOGGER.info("Compiling {} schemas in project with {} schemas found on classpath...", schemas.size(), schemasFoundOnClasspath.size());
    Set<String> fqcnsFoundOnClasspath = schemasFoundOnClasspath.stream().map(Schema::getFullName).collect(Collectors.toSet());
    CodeGenerationConfig codeGenerationConfig = new CodeGenerationConfig(
        config.getStringRepresentation(), config.isAvro702Handling(), avscConfig
    );
    SpecificCompilerHelper.compile(
        schemas,
        config.getOutputSpecificRecordClassesRoot(),
        config.getMinAvroVersion(),
        fqcnsFoundOnClasspath,
        config.getDuplicateSchemasToIgnore(),
        config.getDupBehaviour(),
        codeGenerationConfig
    );

    for (Schema schema : schemas) {
      if (schema.getType() == Schema.Type.RECORD && Avro702Checker.isSusceptible(schema)) {
        if (config.isAvro702Handling()) {
          LOGGER.warn("schema " + schema.getFullName() + " is susceptible to avro-702 and was fixed");
        } else {
          LOGGER.warn("schema " + schema.getFullName() + " is susceptible to avro-702 (avro702 handling disabled)");
        }
      }
    }

    if (config.getOutputExpandedSchemasRoot() != null) {
      LOGGER.info("writing {} expanded schemas", schemas.size());
      for (Schema schema : schemas) {
        FileUtils.writeStringToFile(
            new File(config.getOutputExpandedSchemasRoot(), schema.getFullName() + ".txt"),
            schema.toString(true),
            StandardCharsets.UTF_8
        );
      }
    }

    opContext.addVanillaSchemas(new HashSet<>(schemas), avroFiles);

    LOGGER.info("Compilation complete.");
  }

  private static String fqcnFromSchema(JSONObject schemaJson) throws JSONException { //works for both avsc and pdsc
    String namespace = null;
    if (schemaJson.has("namespace")) {
      //actually optional in avro
      namespace = schemaJson.getString("namespace");
    }
    if (!schemaJson.has("name")) {
      throw new IllegalArgumentException("no \"name\" property in schema " + schemaJson);
    }
    String name = schemaJson.getString("name");
    String fqcn;
    if (name.contains(".") || namespace == null) {
      fqcn = name; //avro spec says ignore namespace if name is a "full name"
    } else {
      fqcn = namespace + "." + name;
    }
    return fqcn;
  }

  private static String readFile(String filePath) {
    try {
      try (FileInputStream is = new FileInputStream(filePath)) {
        return IOUtils.toString(is, StandardCharsets.UTF_8);
      }
    } catch (Exception e) {
      throw new IllegalStateException("trying to read " + filePath, e);
    }
  }
}
