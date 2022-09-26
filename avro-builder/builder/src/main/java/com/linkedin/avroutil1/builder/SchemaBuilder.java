/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.builder.operations.codegen.CodeGenerator;
import com.linkedin.avroutil1.builder.operations.codegen.own.AvroUtilCodeGenOp;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.Operation;
import com.linkedin.avroutil1.builder.operations.codegen.vanilla.VanillaProcessedCodeGenOp;
import com.linkedin.avroutil1.builder.plugins.BuilderPlugin;
import com.linkedin.avroutil1.builder.plugins.BuilderPluginContext;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;


/**
 * generates java code from avsc files
 */
public class SchemaBuilder {

  private SchemaBuilder() { }

  public static void main(String[] args) throws Exception {
    List<BuilderPlugin> plugins = loadPlugins(1);

    OptionParser parser = new OptionParser();

    OptionSpec<String> inputOpt = parser.accepts("input", "Schema or directory of schemas to compile [REQUIRED]")
        .withRequiredArg().required()
        .describedAs("file");
    OptionSpec<String> nonImportableSourceOpt = parser.accepts("non-importable-source", "source schemas that cannot be used as imports by other schemas")
        .withOptionalArg()
        .describedAs("file")
        .withValuesSeparatedBy(File.pathSeparatorChar);
    OptionSpec<String> includeFromClasspathOpt = parser.accepts("includeClasspath", "search for undefined schemas on the classpath")
        .withOptionalArg()
        .describedAs("true/false");
    OptionSpec<String> outputOpt = parser.accepts("output", "Directory to put output code [REQUIRED]")
        .withRequiredArg().required()
        .describedAs("dir");
    OptionSpec<String> expandedSchemas = parser.accepts("expandedSchemas", "Directory for dumping expanded schemas")
        .withOptionalArg()
        .describedAs("dir");
    OptionSpec<String> generator = parser.accepts("generator", "generator to use")
        .withOptionalArg()
        .defaultsTo(CodeGenerator.VANILLA.name())
        .describedAs(Arrays.toString(CodeGenerator.values()));
    OptionSpec<String> minAvroVerOpt = parser.accepts("minAvroVer", "Minimum target avro version. defaults to 1.4")
        .withOptionalArg()
        .describedAs("X.Y");
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
        .defaultsTo("true")
        .describedAs("true/false");

    //allow plugins to add CLI options
    for (BuilderPlugin plugin : plugins) {
      plugin.customizeCLI(parser);
    }

    if (args.length == 0) {
      printHelpAndCroak(parser, "no arguments provided");
    }

    OptionSet options = null;
    try {
      options = parser.parse(args);
    } catch (OptionException e) {
      printHelpAndCroak(parser, e.getMessage());
    }
    assert options != null; //to remove a warning below

    List<File> inputs = AvroSchemaBuilderUtils.toFiles(options.valuesOf(inputOpt));

    List<File> nonImportableSources = null;
    if (options.has(nonImportableSourceOpt)) {
      nonImportableSources = AvroSchemaBuilderUtils.toFiles(options.valuesOf(nonImportableSourceOpt));
    }

    boolean includeFromClasspath = false;
    if (options.has(includeFromClasspathOpt)) {
      String value = options.valueOf(includeFromClasspathOpt);
      if (Boolean.TRUE.equals(Boolean.parseBoolean(value))) {
        includeFromClasspath = true;
      }
    }

    File outputDir = new File(options.valueOf(outputOpt));

    File expandedSchemasDir = null;
    if (options.has(expandedSchemas)) {
      expandedSchemasDir = new File(options.valueOf(expandedSchemas));
      makeSureExists(expandedSchemasDir);
    }

    CodeGenerator generatorType = CodeGenerator.VANILLA;
    if (options.has(generator)) {
      try {
        generatorType = CodeGenerator.valueOf(options.valueOf(generator));
      } catch (IllegalArgumentException e) {
        croak("unknown code generator " + options.valueOf(generator));
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
      if (dupBehaviour == DuplicateSchemaBehaviour.WARN) {
        System.err.println("WARNING: duplicate schemas are being ignored");
      }
    }

    List<String> duplicateClassesToIgnore = new ArrayList<>();
    if (options.has(dupsToIgnore)) {
      duplicateClassesToIgnore = options.valuesOf(dupsToIgnore);
    }

    StringRepresentation stringRepresentation = StringRepresentation.CharSequence; //default matches 1.4 (existing) behaviour
    if (options.has(stringRepresentationOpt)) {
      String stringRepStr = options.valueOf(stringRepresentationOpt);
      try {
        stringRepresentation = StringRepresentation.valueOf(stringRepStr);
      } catch (IllegalArgumentException e) {
        croak("unhandled string representation " + options.valueOf(stringRepStr));
      }
    }

    AvroVersion minAvroVer = AvroVersion.AVRO_1_4;
    if (options.has(minAvroVerOpt)) {
      try {
        minAvroVer = AvroVersion.fromSemanticVersion(options.valueOf(minAvroVerOpt));
      } catch (IllegalArgumentException e) {
        croak("unhandled major avro version " + options.valueOf(minAvroVerOpt));
      }
    }

    boolean handleAvro702 = true;
    if (options.has(enableAvro702Handling)) {
      String value = options.valueOf(enableAvro702Handling);
      handleAvro702 = Boolean.TRUE.equals(Boolean.parseBoolean(value));
    }

    //allow plugins to parse and validate their own added options
    for (BuilderPlugin plugin : plugins) {
      plugin.parseAndValidateOptions(options);
    }

    BuilderPluginContext context = new BuilderPluginContext();

    CodeGenOpConfig opConfig = new CodeGenOpConfig(
        inputs,
        nonImportableSources,
        includeFromClasspath,
        outputDir,
        expandedSchemasDir,
        generatorType,
        dupBehaviour,
        duplicateClassesToIgnore,
        stringRepresentation,
        minAvroVer,
        handleAvro702
    );

    opConfig.validateParameters();

    Operation op;
    switch (opConfig.getGeneratorType()) {
      case AVRO_UTIL:
        op = new AvroUtilCodeGenOp(opConfig);
        break;
      case VANILLA:
        op = new VanillaProcessedCodeGenOp(opConfig);
        break;
      default:
        throw new IllegalStateException("unhandled: " + opConfig.getGeneratorType());
    }
    context.add(op);

    //allow plugins to add operations
    for (BuilderPlugin plugin : plugins) {
      plugin.createOperations(context);
    }

    context.run();
  }

  private static List<BuilderPlugin> loadPlugins(@SuppressWarnings("SameParameterValue") int currentApiVersion) {
    List<BuilderPlugin> plugins = new ArrayList<>(1);
    ServiceLoader<BuilderPlugin> loader = ServiceLoader.load(BuilderPlugin.class);
    for (BuilderPlugin plugin : loader) {
      Set<Integer> pluginSupportedVersions = plugin.supportedApiVersions();
      if (!pluginSupportedVersions.contains(currentApiVersion)) {
        System.err.println("plugin " + plugin.name() + " does not support current API version " + currentApiVersion
            + " and will not be loaded");
        continue;
      }
      plugins.add(plugin);
    }
    //TODO - add a mechanism to allow users to disable any plugins found via CLI options
    return plugins;
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

  private static void croak(String message) {
    croak(message, 1);
  }

  private static void croak(String message, int exitCode) {
    System.err.println(message);
    System.exit(exitCode);
  }
}
