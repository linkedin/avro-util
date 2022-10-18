package com.linkedin.avroutil1.builder;

import build.generated.SpamUser;
import com.linkedin.avroutil1.compatibility.AvroCodecUtil;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroRecordUtil;
import com.linkedin.avroutil1.compatibility.RecordConversionConfig;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Tester {
  @Test
  public void testSimpleProjectUsingVanilla() throws Exception {
    File simpleProjectRoot = new File(locateTestProjectsRoot(), "tester");
    File inputFolder = new File(simpleProjectRoot, "input");
    File outputFolder = new File(simpleProjectRoot, "output");
    if (outputFolder.exists()) { //clear output
      FileUtils.deleteDirectory(outputFolder);
    }
    //run the builder
    SchemaBuilder.main(new String[] {
        "--input", inputFolder.getAbsolutePath(),
        "--output", outputFolder.getAbsolutePath()});
    //see output was generated
    List<Path> javaFiles = Files.find(outputFolder.toPath(), 5,
        (path, basicFileAttributes) -> path.getFileName().toString().endsWith(".java")
    ).collect(Collectors.toList());
    Assert.assertEquals(javaFiles.size(), 1);

//    Schema newSchema = getNewSchema();
//    String newJson = "{\"f\" : \"NEW_ENUM\"}";
//    GenericRecord genericRecord = AvroCodecUtil.deserializeAsGeneric(newJson, newSchema, newSchema);
//    SpamUser spamUser = AvroRecordUtil.genericRecordToSpecificRecord(genericRecord, null,
//        new RecordConversionConfig(true, true, true, true, StringRepresentation.Utf8, true));
//    Assert.assertEquals("UNKNOWN", spamUser.f.name());
  }

  private Schema getNewSchema() {
    return AvroCompatibilityHelper.parse("{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"SpamUser\",\n"
        + "  \"namespace\": \"build.generated\",\n"
        + "   \"fields\": [\n"
        + "      {\n"
        + "        \"name\": \"f\",\n"
        + "        \"type\": {\n"
        + "                  \"type\": \"enum\",\n"
        + "                  \"name\": \"SpamType\",\n"
        + "                  \"namespace\": \"build.generated\",\n"
        + "                  \"symbols\": [\n"
        + "                     \"FIRST_ENUM\",\n"
        + "                     \"UNKNOWN\",\n"
        + "                     \"NEW_ENUM\"\n"
        + "                  ],\n"
        + "                  \"default\": \"UNKNOWN\"\n"
        + "                }\n"
        + "      }\n"
        + "   ]\n"
        + "}\n");

  }

  private File locateTestProjectsRoot() {
    //the current working directory for test execution varies across gradle and IDEs.
    //as such, we need to get creative to locate the folder
    URL url = Thread.currentThread().getContextClassLoader().getResource("test-projects");
    if (url == null) {
      throw new IllegalStateException("unable to find \"test-projects\" folder");
    }
    if (!url.getProtocol().equals("file")) {
      throw new IllegalStateException(url + " is a " + url.getProtocol() + " and not a file/folder");
    }
    File file = new File(url.getPath()); //getPath() should be an absolute path string
    if (!file.exists()) {
      throw new IllegalStateException("test-projects root folder " + file.getAbsolutePath() + " does not exist");
    }
    if (!file.isDirectory() || !file.canRead()) {
      throw new IllegalStateException(
          "test-projects root folder " + file.getAbsolutePath() + " not a folder or is unreadable");
    }
    return file;
  }
}
