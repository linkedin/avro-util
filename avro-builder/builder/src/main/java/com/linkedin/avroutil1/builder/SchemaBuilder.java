/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder;

import com.linkedin.avroutil1.builder.operations.OperationContext;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenerator;
import com.linkedin.avroutil1.builder.operations.codegen.OperationContextBuilder;
import com.linkedin.avroutil1.builder.operations.codegen.own.AvroUtilCodeGenPlugin;
import com.linkedin.avroutil1.builder.operations.codegen.CodeGenOpConfig;
import com.linkedin.avroutil1.builder.operations.codegen.own.AvroUtilOperationContextBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * generates java code from avsc files
 */
public class SchemaBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaBuilder.class);

  private SchemaBuilder() { }

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();

    long pluginLoadStart = System.currentTimeMillis();
    List<BuilderPlugin> plugins = loadPlugins(1);
    long pluginLoadEnd = System.currentTimeMillis();
    LOGGER.info("Loaded {} plugins in {} millis.", plugins.size(), pluginLoadEnd - pluginLoadStart);

    long optionParseStart = System.currentTimeMillis();
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
    OptionSpec<String> resolverPathOpt = parser.accepts("resolverPath", "Resolver path for dependent schemas")
        .withOptionalArg()
        .describedAs("file")
        .withValuesSeparatedBy(File.pathSeparatorChar);
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
    OptionSpec<String> methodStringRepresentationOpt = parser.accepts("methodStringRepresentation", "controls the java type used to represent string in method argument"
            + " in generated code. values are [CharSequence|String|Utf8], default is String")
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

    OptionSpec<String> enableUtf8EncodingInPutByIndex = parser.accepts(
            "enableUtf8EncodingInPutByIndex",
            "enable encoding String/ListOf/MapOf to their Utf8 values during set value in put-by-index method [ put(int, Object) ]")
        .withOptionalArg()
        .defaultsTo("true")
        .describedAs("true/false");

    OptionSpec<String> skipCodegenIfSchemaOnClasspathOpt = parser.accepts("skipCodegenIfSchemaOnClasspath",
            "skips codegen for a schema if it is already present in the classpath. Only applicable for AVRO_UTIL generator."
                + " Use with care! Make sure your classpath only contains the schemas you want to use.")
        .withOptionalArg()
        .defaultsTo("false")
        .describedAs("true/false");

    OptionSpec<String> enableUtf8Encoding = parser.accepts("enableUtf8Encoding", "enable encoding strings to their utf8 values throughout generated code.")
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

    List<File> resolverPath = null;
    if (options.has(resolverPathOpt)) {
      resolverPath = AvroSchemaBuilderUtils.toFiles(options.valuesOf(resolverPathOpt));
    }

    if (includeFromClasspath && resolverPath != null) {
      throw new IllegalStateException("The \"includeClasspath\" and \"resolverPath\" are mutually exclusive");
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

    StringRepresentation methodStringRepresentation = StringRepresentation.String;
    if (options.has(methodStringRepresentationOpt)) {
      String methodStringRepStr = options.valueOf(methodStringRepresentationOpt);
      try {
        methodStringRepresentation = StringRepresentation.valueOf(methodStringRepStr);
      } catch (IllegalArgumentException e) {
        croak("unhandled string methodStringRepresentation " + options.valueOf(methodStringRepStr));
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

    boolean handleUtf8EncodingInPutByIndex = true;
    if (options.has(enableUtf8EncodingInPutByIndex)) {
      String value = options.valueOf(enableUtf8EncodingInPutByIndex);
      handleUtf8EncodingInPutByIndex = Boolean.TRUE.equals(Boolean.parseBoolean(value));
    }

    boolean skipCodegenIfSchemaOnClasspath = false;
    if (options.has(skipCodegenIfSchemaOnClasspathOpt)) {
      String value = options.valueOf(skipCodegenIfSchemaOnClasspathOpt);
      skipCodegenIfSchemaOnClasspath = Boolean.TRUE.equals(Boolean.parseBoolean(value));
    }

    boolean handleUtf8Encoding = true;
    if (options.has(enableUtf8Encoding)) {
      String value = options.valueOf(enableUtf8Encoding);
      handleUtf8Encoding = Boolean.TRUE.equals(Boolean.parseBoolean(value));
      if (methodStringRepresentation.equals(StringRepresentation.CharSequence) && stringRepresentation.equals(StringRepresentation.CharSequence)) {
        handleUtf8EncodingInPutByIndex = handleUtf8Encoding;
      }
    }

    //allow plugins to parse and validate their own added options
    for (BuilderPlugin plugin : plugins) {
      plugin.parseAndValidateOptions(options);
    }

    CodeGenOpConfig opConfig = new CodeGenOpConfig(
        inputs,
        nonImportableSources,
        includeFromClasspath,
        outputDir,
        expandedSchemasDir,
        resolverPath,
        generatorType,
        dupBehaviour,
        duplicateClassesToIgnore,
        stringRepresentation,
        methodStringRepresentation,
        minAvroVer,
        handleAvro702,
        handleUtf8EncodingInPutByIndex,
        skipCodegenIfSchemaOnClasspath,
        handleUtf8Encoding
    );

    opConfig.validateParameters();
    long optionParseEnd = System.currentTimeMillis();
    LOGGER.info("Parsed all options in {} millis.", optionParseEnd - optionParseStart);

    long operationContextBuildStart = System.currentTimeMillis();
    OperationContextBuilder operationContextBuilder;
    switch (opConfig.getGeneratorType()) {
      case AVRO_UTIL:
        operationContextBuilder = new AvroUtilOperationContextBuilder();
        plugins.add(new AvroUtilCodeGenPlugin(opConfig));
        break;
      case VANILLA:
        operationContextBuilder = new VanillaProcessedCodeGenOp();
        break;
      default:
        throw new IllegalStateException("unhandled: " + opConfig.getGeneratorType());
    }
    OperationContext opContext = operationContextBuilder.buildOperationContext(opConfig);
    long operationContextBuildEnd = System.currentTimeMillis();
    LOGGER.info("Built operation context in {} millis.", operationContextBuildEnd - operationContextBuildStart);

    BuilderPluginContext context = new BuilderPluginContext(opContext);

    // Allow other plugins to add operations
    for (BuilderPlugin plugin : plugins) {
      plugin.createOperations(context);
    }

    context.run();

    long end = System.currentTimeMillis();
    LOGGER.info("Finished running SchemaBuilder in {} millis", end - start);
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
