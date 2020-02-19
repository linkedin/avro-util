package com.linkedin.avroutil1;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;


public class TestTool {

  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0) {
      System.err.println("at least one argument required");
      System.exit(1);
    }
    String command = args[0].toLowerCase(Locale.ROOT).trim();
    List<String> rest = Arrays.asList(args).subList(1, args.length);
    switch (command) {
      case "compile":
        generateSpecificClasses(rest);
        break;
      default:
        System.err.println("unrecognized command " + command + ": known commands are \"compile\"");
        System.exit(1);
    }
  }

  public static void generateSpecificClasses(List<String> args) throws IOException {
    if (args.size() != 2) {
      System.err.println("compile expects 2 arguments - source file and destination root folder");
      System.exit(1);
    }
    File input = new File(args.get(0));
    if (!input.exists() || !input.isFile() || !input.canRead()) {
      System.err.println("input file " + input.getAbsolutePath() + " does not exist or is not readable");
      System.exit(1);
    }
    File output = new File(args.get(1));
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
        AvroVersion.earliest(), AvroVersion.latest()
    );
    for (AvroGeneratedSourceCode sourceFile : compilationResults) {
      File fileOnDisk = sourceFile.writeToDestination(output);
      System.out.println("(over)wrote " + fileOnDisk.getAbsolutePath());
    }
  }
}
