/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


/**
 * a command line tool to generate code for a single avro schema file using the helper.
 * this utility exists to generate source code for tests inside this project only.
 */
public class TestTool {

  static class Arguments {
    @Option(name  = "-op", required = true)
    private String operation;
    @Option(name = "-min")
    private AvroVersion minVer = AvroVersion.AVRO_1_4;
    @Option(name = "-in", required = true)
    private File input;
    @Option(name = "-out", required = true)
    private File output;
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments =  parse(args);
    switch (arguments.operation) {
      case "compile":
        generateSpecificClasses(arguments.input, arguments.output, arguments.minVer);
        break;
      default:
        System.err.println("unrecognized operation " + arguments.operation + ": known operations are \"compile\"");
        System.exit(1);
    }
  }

  private static Arguments parse(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CmdLineParser parser = new CmdLineParser(arguments);
    parser.parseArgument(args);
    return arguments;
  }

  public static void generateSpecificClasses(File input, File output, AvroVersion minVer) throws IOException {
    if (!input.exists() || !input.isFile() || !input.canRead()) {
      System.err.println("input file " + input.getAbsolutePath() + " does not exist or is not readable");
      System.exit(1);
    }
    if (output.exists()) {
      if (!output.isDirectory() || !output.canWrite()) {
        System.err.println("output root " + output.getAbsolutePath() + " is not a folder or is not writeable");
        System.exit(1);
      }
    } else {
      if (!output.mkdirs()) {
        System.err.println("unable to create output root " + output.getAbsolutePath());
        System.exit(1);
      }
    }
    String avsc;
    try (FileInputStream is = new FileInputStream(input)) {
      avsc = IOUtils.toString(is, StandardCharsets.UTF_8);
      System.out.println("read " + input.getAbsolutePath());
    }
    Schema schema = AvroCompatibilityHelper.parse(avsc);
    Collection<AvroGeneratedSourceCode> compilationResults = AvroCompatibilityHelper.compile(
        Collections.singletonList(schema),
        minVer,
        AvroVersion.latest()
    );
    for (AvroGeneratedSourceCode sourceFile : compilationResults) {
      File fileOnDisk = sourceFile.writeToDestination(output);
      System.out.println("(over)wrote " + fileOnDisk.getAbsolutePath());
    }
  }
}
