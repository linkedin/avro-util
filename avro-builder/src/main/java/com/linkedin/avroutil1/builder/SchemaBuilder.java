/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.compatibility.Avro702Checker;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * generates java code from avsc files
 */
public class SchemaBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaBuilder.class);
  private static final String AVRO_EXTENSION = "avsc";
  private static final Charset UTF8 = Charset.forName("UTF-8");

  private SchemaBuilder() { }

  public static void main(String[] args) throws Exception {
    makeSureAvroCompilerExists();
    OptionParser parser = new OptionParser();
    OptionSpec<String> expandedSchemas = parser.accepts("expandedSchemas", "Directory for dumping expanded schemas")
        .withOptionalArg()
        .describedAs("dir");
    OptionSpec<String> includesOpt = parser.accepts("include", "Common schemas")
        .withOptionalArg()
        .describedAs("file")
        .withValuesSeparatedBy(File.pathSeparatorChar);
    OptionSpec<String> outputOpt = parser.accepts("output", "Directory to put output code [REQUIRED]")
        .withRequiredArg()
        .describedAs("dir");
    OptionSpec<String> inputOpt = parser.accepts("input", "Schema or directory of schemas to compile [REQUIRED]")
        .withRequiredArg()
        .describedAs("file");
    OptionSpec<String> minAvroVerOpt = parser.accepts("minAvroVer", "Minimum target avro version. defaults to 1.4")
        .withOptionalArg()
        .describedAs("X.Y");
    OptionSpec<String> includeFromClasspathOpt = parser.accepts("includeClasspath", "search for undefined schemas on the classpath")
        .withOptionalArg()
        .describedAs("true/false");
    OptionSpec<String> stringRepresentationOpt = parser.accepts("stringRepresentation", "controls the java type used to represent string"
            + " properties in generated code. values are [CharSequence|String|Utf8], default is CharSequence for compatibility with 1.4")
        .withOptionalArg();
    OptionSpec<String> failOnDupsOpt = parser.accepts("onDups", "control behaviour on encountering duplicate schema definitions")
        .withOptionalArg()
        .defaultsTo(DuplicateSchemaBehaviour.FAIL_IF_DIFFERENT.name())
        .describedAs(Arrays.toString(DuplicateSchemaBehaviour.values()));
    OptionSpec<String> dupsToIgnore = parser.accepts("dupsToIgnore", "Ignores duplicates for supplied"
            + " classes. Provide comma separated fully qualified class names that need to be ignored for dups. "
            + "example: com.linkedin.package.MyClass,com.linkedin.package2.YourClass")
        .withOptionalArg()
        .withValuesSeparatedBy(',');
    OptionSpec<String> enableAvro702Handling = parser.accepts(
            "enableAvro702Handling",
            "enable handling of avro702 when generating classes (will have correct AVSC with aliases to the impacted fullnames)")
        .withOptionalArg()
        .defaultsTo("false")
        .describedAs("true/false");

    OptionSet options = null;
    try {
      options = parser.parse(args);
    } catch (OptionException e) {
      printHelpAndCroak(parser, e.getMessage());
    }

    assert options != null; //to remove a warning below

    if (args.length == 0) {
      printHelpAndCroak(parser, "");
    }

    if (!options.has(inputOpt) || !options.has(outputOpt)) {
      printHelpAndCroak(parser, "Missing required argument(s) for input and/or output");
    }
    List<File> inputs = AvroSchemaBuilderUtils.toFiles(options.valuesOf(inputOpt));
    File outputDir = new File(options.valueOf(outputOpt));
    if (outputDir.exists() && (!outputDir.isDirectory() || !outputDir.canWrite())) {
      croak("output folder " + outputDir + " either isnt a folder or is not writable");
    }
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      croak("output folder " + outputDir + " does not exist and we failed to create it");
    }

    AvroVersion minAvroVer = AvroVersion.AVRO_1_4;
    if (options.has(minAvroVerOpt)) {
      try {
        minAvroVer = AvroVersion.fromSemanticVersion(options.valueOf(minAvroVerOpt));
      } catch (IllegalArgumentException e) {
        croak("unhandled major avro version " + options.valueOf(minAvroVerOpt));
      }
    }

    List<File> includes = new ArrayList<>();
    if (options.has(includesOpt)) {
      includes = AvroSchemaBuilderUtils.toFiles(options.valuesOf(includesOpt));
      //input and includes should not overlap - meaning neither should be a parent folder of the other
      //(otherwise schemas will be picked up twice and the run will fail for duplicates)
      //the following is brutish, but how many input folders do we really expect ? :-)
      List<File> inputsAndIncludes = new ArrayList<>(inputs);
      inputsAndIncludes.addAll(includes);
      for (File anyFile : inputsAndIncludes) {
        for (File anyOtherFile : inputsAndIncludes) {
          if (anyFile == anyOtherFile) {
            continue;
          }
          if (AvroSchemaBuilderUtils.isParentOf(anyOtherFile, anyFile)) { //other direction will be covered later in the loop
            croak("input/include paths " + anyFile + " and " + anyOtherFile + " overlap");
          }
        }
      }
    }

    StringRepresentation stringRepresentation = StringRepresentation.CharSequence; //default matches 1.4 (existing) behaviour
    String stringRepStr = null;
    if (options.has(stringRepresentationOpt)) {
      stringRepStr = options.valueOf(stringRepresentationOpt);
    }
    if (stringRepStr != null) {
      try {
        stringRepresentation = StringRepresentation.valueOf(stringRepStr);
      } catch (IllegalArgumentException e) {
        croak("unhandled string representation " + options.valueOf(stringRepStr));
      }
    }

    //build a classpath SchemaSet if classpath (cp) lookup is turned on
    ClasspathSchemaSet cpLookup = null;
    if (options.has(includeFromClasspathOpt)) {
      String value = options.valueOf(includeFromClasspathOpt);
      if (Boolean.TRUE.equals(Boolean.parseBoolean(value))) {
        cpLookup = new ClasspathSchemaSet();
      }
    }

    DuplicateSchemaBehaviour dupBehaviour = DuplicateSchemaBehaviour.FAIL_IF_DIFFERENT;
    if (options.has(failOnDupsOpt)) {
      String value = options.valueOf(failOnDupsOpt);
      try {
        dupBehaviour = DuplicateSchemaBehaviour.valueOf(value);
      } catch (IllegalArgumentException e) {
        croak("unknown value for DuplicateSchemaBehaviour - " + value
            + ". known values are " + Arrays.toString(DuplicateSchemaBehaviour.values()));
      }
    }
    if (dupBehaviour == DuplicateSchemaBehaviour.WARN) {
      //TODO - remove last sentence once reward is claimed
      System.err.println("WARNING: duplicate schemas are being ignored. this is generally a bad idea since last writer would win and"
          + " the order of schema traversal is undefined. first user to read this can ping rrosenbl");
    }

    boolean handleAvro702 = false;
    if (options.has(enableAvro702Handling)) {
      String value = options.valueOf(enableAvro702Handling);
      handleAvro702 = Boolean.TRUE.equals(Boolean.parseBoolean(value));
    }

    AvscGenerationConfig avscConfig = AvscGenerationConfig.VANILLA_ONELINE;
    Set<String> schemasToGenerateBadly = Collections.emptySet(); //by default
    if (handleAvro702) {
      avscConfig = AvscGenerationConfig.CORRECT_MITIGATED_ONELINE;
    }

    FileSystemSchemaSetProvider provider = new FileSystemSchemaSetProvider(inputs, includes,
        FileSystemSchemaSetProvider.DEFAULT_SCHEMA_SUFFIX, cpLookup);
    //load ALL the schemas (deeply-nested ones included) to allow discovery/import
    //of inner schemas
    List<Schema> schemas = provider.loadSchemas(false).getAll();
    List<Schema> schemasFoundOnClasspath = cpLookup != null ? cpLookup.getAll() : Collections.emptyList();
    if (!schemasFoundOnClasspath.isEmpty()) {
      StringJoiner csv = new StringJoiner(", ");
      for (Schema onCP : schemasFoundOnClasspath) {
        csv.add(onCP.getFullName());
      }
      LOGGER.info("the following schemas were found on the classpath and will not be generated: " + csv.toString());
      schemas.removeAll(schemasFoundOnClasspath);
    }

    Set<File> avroFiles = new HashSet<>();
    String[] extensions = new String[]{AVRO_EXTENSION};
    for (File include : includes) {
      avroFiles.addAll(FileUtils.listFiles(include, extensions, true));
    }
    for (File file : inputs) {
      if (file.isDirectory()) {
        avroFiles.addAll(FileUtils.listFiles(file, extensions, true));
      } else {
        avroFiles.add(file);
      }
    }

    List<String> duplicateClassesToIgnore = new ArrayList<>();
    if (options.has(dupsToIgnore)) {
      duplicateClassesToIgnore = options.valuesOf(dupsToIgnore);
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
        if (schemaNameToFilepath.containsKey(fqcn) && !duplicateClassesToIgnore.contains(fqcn)) {
          String otherPath = schemaNameToFilepath.get(fqcn);
          switch (dupBehaviour) {
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
              throw new IllegalStateException("unhandled: " + dupBehaviour);
          }
        }
        schemaNameToFilepath.put(fqcn, filePath);
      }
    }

    LOGGER.info("Compiling {} schemas in project with {} schemas found on classpath...", schemas.size(), schemasFoundOnClasspath.size());
    Set<String> fqcnsFoundOnClasspath = schemasFoundOnClasspath.stream().map(Schema::getFullName).collect(Collectors.toSet());
    CodeGenerationConfig codeGenerationConfig = new CodeGenerationConfig(
        stringRepresentation, handleAvro702, avscConfig
    );
    SpecificCompilerHelper.compile(schemas, outputDir, minAvroVer, fqcnsFoundOnClasspath, duplicateClassesToIgnore, dupBehaviour, codeGenerationConfig);
    if (options.has(expandedSchemas)) {
      File expandedSchemasDir = new File(options.valueOf(expandedSchemas));
      makeSureExists(expandedSchemasDir);
      for (Schema schema : schemas) {
        FileUtils.writeStringToFile(new File(expandedSchemasDir, schema.getFullName() + ".txt"), schema.toString(true), UTF8);
      }
    }
    for (Schema schema : schemas) {
      if (schema.getType() == Schema.Type.RECORD && Avro702Checker.isSusceptible(schema)) {
        if (handleAvro702) {
          if (schemasToGenerateBadly.contains(schema.getFullName())) {
            LOGGER.warn("WARNING: schema " + schema.getFullName() + " is susceptible to avro-702 and was purposefully generated badly");
          } else {
            LOGGER.warn("WARNING: schema " + schema.getFullName() + " is susceptible to avro-702 and was fixed (this time)");
          }
        } else {
          LOGGER.warn("WARNING: schema " + schema.getFullName() + " is susceptible to avro-702 (avro702 handling disabled)");
        }
      }
    }
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
    String fqcn ;
    if (name.contains(".") || namespace == null) {
      fqcn = name; //avro spec says ignore namespace if name is a "full name"
    } else {
      fqcn = namespace + "." + name;
    }
    return fqcn;
  }

  private static void printHelpAndCroak(OptionParser parser, String message) throws IOException {
    parser.printHelpOn(System.err);
    croak(message);
  }

  private static void makeSureExists(File dir) {
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IllegalStateException("unable to create destination folder " + dir);
    }
  }

  private static void makeSureAvroCompilerExists() {
    AvroVersion compilerVersion = AvroCompatibilityHelper.getRuntimeAvroCompilerVersion();
    if (compilerVersion == null) {
      croak("no avro-compiler on the classpath. please add some version of avro-compiler.jar to the classpath");
    }
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

  private static void croak(String message) {
    croak(message, 1);
  }

  private static void croak(String message, int exitCode) {
    System.err.println(message);
    System.exit(exitCode);
  }
}
