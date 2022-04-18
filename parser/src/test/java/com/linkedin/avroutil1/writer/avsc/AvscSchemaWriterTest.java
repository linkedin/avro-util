/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.writer.avsc;

import com.linkedin.avroutil1.compatibility.HelperConsts;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.parser.avsc.AvscIssue;
import com.linkedin.avroutil1.parser.avsc.AvscParseResult;
import com.linkedin.avroutil1.parser.avsc.AvscParser;
import java.io.File;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvscSchemaWriterTest {

  @Test
  public void testWritingPrimitiveSchemas() throws Exception {

    //simplest

    testParsingCycle("\"null\"");
    testParsingCycle("\"boolean\"");
    testParsingCycle("\"int\"");
    testParsingCycle("\"long\"");
    testParsingCycle("\"float\"");
    testParsingCycle("\"double\"");
    testParsingCycle("\"string\"");
    testParsingCycle("\"bytes\"");

    //"fatter"

    testParsingCycle("{\"type\": \"null\"}");
    testParsingCycle("{\"type\": \"boolean\"}");
    testParsingCycle("{\"type\": \"int\"}");
    testParsingCycle("{\"type\": \"long\"}");
    testParsingCycle("{\"type\": \"float\"}");
    testParsingCycle("{\"type\": \"double\"}");
    testParsingCycle("{\"type\": \"string\"}");
    testParsingCycle("{\"type\": \"bytes\"}");

    //with props and logical types

    testParsingCycle("{\"type\": \"null\", \"prop1\": null}");
    testParsingCycle("{\"type\": \"boolean\", \"prop2\": 45.6}");
    testParsingCycle("{\"type\": \"int\", \"logicalType\": \"time-millis\"}");
    testParsingCycle("{\"type\": \"long\", \"logicalType\": \"timestamp-micros\"}");
    testParsingCycle("{\"type\": \"float\", \"prop3\": \"string value\"}");
    testParsingCycle("{\"type\": \"double\", \"prop4\": [1, null, false, \"str\"]}");
    testParsingCycle("{\"type\": \"string\", \"avro.java.string\": \"String\"}");
    testParsingCycle("{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}");
  }

  @Test
  public void testWritingContainerSchemas() throws Exception {
    testParsingCycle("{\"type\": \"array\", \"items\": \"string\"}");
    testParsingCycle("{\"type\": \"map\", \"values\": \"long\"}");
    testParsingCycle("[\"int\", \"null\"]");
    testParsingCycle("[{\"type\": \"array\", \"items\": {\"type\": \"map\", \"values\": \"long\"}}, {\"type\": \"map\", \"values\": \"bytes\"}, \"string\"]");
  }

  @Test
  public void testWritingSimpleNamedSchemas() throws Exception {
    testParsingCycle("{\"type\": \"enum\", \"namespace\": \"ns\", \"name\": \"MyEnum\", \"doc\": \"doc\", \"symbols\": [\"A\", \"B\"], \"default\": \"B\"}");
    testParsingCycle("{\"type\": \"fixed\", \"name\": \"MyFixed\", \"size\": 7, \"aliases\": [\"some.OtherName\"]}");
    testParsingCycle(
        "{\n"
        + "    \"type\": \"record\",\n"
        + "    \"name\": \"SimpleRecord\",\n"
        + "    \"fields\": [\n"
        + "        {\"name\": \"f1\", \"type\": \"int\", \"default\": 42}\n"
        + "    ]\n"
        + "}"
    );
  }

  /**
   * given an avsc, parses and re-prints it using our code
   * and compares the result to vanilla avro.
   * @param avsc
   */
  private void testParsingCycle(String avsc) {
    Schema reference = Schema.parse(avsc);

    AvscParser parser = new AvscParser();
    AvscParseResult parseResults = parser.parse(avsc);
    List<AvscIssue> parseIssues = parseResults.getIssues();
    Assert.assertTrue(parseIssues == null || parseIssues.isEmpty(), "parse issues: " + parseIssues);
    AvroSchema parsed = parseResults.getTopLevelSchema();
    Assert.assertNotNull(parsed);
    AvscSchemaWriter writer = new AvscSchemaWriter();
    AvscFile file = writer.writeSingle(parsed);
    Assert.assertNotNull(file);
    if (HelperConsts.NAMED_TYPES.contains(reference.getType())){
      //for named schemas the file path is determined by schema name
      Assert.assertNotNull(file.getPathFromRoot());
      String expectedFileName = reference.getFullName().replaceAll("\\.", Matcher.quoteReplacement(File.separator)) + ".avsc";
      Assert.assertEquals(file.getPathFromRoot().toString(), expectedFileName);
    } else {
      //cant auto-name files containing other schema types
      Assert.assertNull(file.getPathFromRoot());
    }
    String avsc2 = file.getContents();

    Schema afterCycle = Schema.parse(avsc2);

    Assert.assertEquals(reference, afterCycle);
  }
}
