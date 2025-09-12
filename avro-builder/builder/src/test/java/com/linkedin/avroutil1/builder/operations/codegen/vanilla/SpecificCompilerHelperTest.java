/*
 * Copyright 2025 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.operations.codegen.vanilla;

import com.linkedin.avroutil1.builder.DuplicateSchemaBehaviour;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SpecificCompilerHelperTest {

  private static Schema parse(String schemaJson) throws Exception {
    try (InputStream in = new ByteArrayInputStream(schemaJson.getBytes(StandardCharsets.UTF_8))) {
      return AvroCompatibilityHelper.parse(in, SchemaParseConfiguration.LOOSE, null).getMainSchema();
    }
  }

  private static File tempDir(String prefix) throws Exception {
    return Files.createTempDirectory(prefix).toFile();
  }

  @Test
  public void testIdenticalDuplicateSchemas_generateOnce_noThrow() throws Exception {
    String schemaJson = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"int\" } ]\n" +
        "}";

    Schema s1 = parse(schemaJson);
    Schema s2 = parse(schemaJson); // identical duplicate

    List<Schema> schemas = Arrays.asList(s1, s2);
    File out = tempDir("avro-gen-identical");

    List<File> files = SpecificCompilerHelper.compile(
        schemas,
        out,
        AvroVersion.AVRO_1_9,
        Collections.emptyList(),
        Collections.emptyList(),
        DuplicateSchemaBehaviour.FAIL_IF_DIFFERENT,
        CodeGenerationConfig.COMPATIBLE_DEFAULTS
    );

    // Only one class should be generated
    Assert.assertEquals(files.size(), 1, "Expected only one generated file for identical duplicates");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testDifferentDuplicateSchemas_failByDefault() throws Exception {
    String schemaJsonV1 = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"int\" } ]\n" +
        "}";
    String schemaJsonV2 = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"long\" } ]\n" +
        "}"; // same name, different field type

    Schema s1 = parse(schemaJsonV1);
    Schema s2 = parse(schemaJsonV2);

    List<Schema> schemas = Arrays.asList(s1, s2);
    File out = tempDir("avro-gen-different-fail");

    // Default behavior with FAIL_IF_DIFFERENT should throw when encountering differing duplicates
    SpecificCompilerHelper.compile(
        schemas,
        out,
        AvroVersion.AVRO_1_9,
        Collections.emptyList(),
        Collections.emptyList(),
        DuplicateSchemaBehaviour.FAIL_IF_DIFFERENT,
        CodeGenerationConfig.COMPATIBLE_DEFAULTS
    );
  }

  @Test
  public void testDifferentDuplicateSchemas_warnModeGeneratesOnce() throws Exception {
    String schemaJsonV1 = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"int\" } ]\n" +
        "}";
    String schemaJsonV2 = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"long\" } ]\n" +
        "}"; // different

    Schema s1 = parse(schemaJsonV1);
    Schema s2 = parse(schemaJsonV2);

    List<Schema> schemas = Arrays.asList(s1, s2);
    File out = tempDir("avro-gen-different-warn");

    List<File> files = SpecificCompilerHelper.compile(
        schemas,
        out,
        AvroVersion.AVRO_1_9,
        Collections.emptyList(),
        Collections.emptyList(),
        DuplicateSchemaBehaviour.WARN,
        CodeGenerationConfig.COMPATIBLE_DEFAULTS
    );

    // Only first schema should be generated
    Assert.assertEquals(files.size(), 1, "Expected only one generated file under WARN for differing duplicates");
  }

  @Test
  public void testIgnoreDups_allowsDifferences() throws Exception {
    String schemaJsonV1 = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"int\" } ]\n" +
        "}";
    String schemaJsonV2 = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"long\" } ]\n" +
        "}";

    Schema s1 = parse(schemaJsonV1);
    Schema s2 = parse(schemaJsonV2);

    List<Schema> schemas = Arrays.asList(s1, s2);
    File out = tempDir("avro-gen-ignore");

    Collection<String> ignore = Collections.singletonList("com.example.MyRec");

    List<File> files = SpecificCompilerHelper.compile(
        schemas,
        out,
        AvroVersion.AVRO_1_9,
        Collections.emptyList(),
        ignore,
        DuplicateSchemaBehaviour.FAIL_IF_DIFFERENT,
        CodeGenerationConfig.COMPATIBLE_DEFAULTS
    );

    Assert.assertEquals(files.size(), 1, "Expected only one generated file when differences are ignored");
  }

  @Test
  public void testJsonPropsToIgnore_treatedEqual() throws Exception {
    String schemaJsonBase = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"int\" } ]\n" +
        "}";
    String schemaJsonWithProp = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"MyRec\",\n" +
        "  \"namespace\": \"com.example\",\n" +
        "  \"fields\": [ { \"name\": \"a\", \"type\": \"int\" } ],\n" +
        "  \"my_custom_prop\": \"xyz\"\n" +
        "}"; // differs only by a custom json prop

    Schema s1 = parse(schemaJsonBase);
    Schema s2 = parse(schemaJsonWithProp);

    List<Schema> schemas = Arrays.asList(s1, s2);
    File out = tempDir("avro-gen-jsonprop-ignore");

    Set<String> jsonPropsToIgnore = new HashSet<>();
    jsonPropsToIgnore.add("my_custom_prop");

    List<File> files = SpecificCompilerHelper.compile(
        schemas,
        out,
        AvroVersion.AVRO_1_9,
        Collections.emptyList(),
        Collections.emptyList(),
        DuplicateSchemaBehaviour.WARN,
        CodeGenerationConfig.COMPATIBLE_DEFAULTS,
        jsonPropsToIgnore
    );

    Assert.assertEquals(files.size(), 1, "Expected only one generated file when differing only by ignored json props");
  }
}
