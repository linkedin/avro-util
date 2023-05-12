/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.Jackson1JsonGeneratorWrapper;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import com.linkedin.avroutil1.compatibility.JsonGeneratorWrapper;
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.codehaus.jackson.JsonGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro14AvscWriterTest {

  @Test
  public void testLegacyOneLine() throws IOException {
    // Only string and serialized json props retained in avro 1.4
    String expected = "{\"type\":\"record\",\"name\":\"RecordWithFieldProps\",\"namespace\":\"com.acme\",\"doc\":\"A perfectly normal record with field props\",\"fields\":[{\"name\":\"stringField\",\"type\":\"string\",\"nestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\",\"stringProp\":\"stringValue\"},{\"name\":\"intField\",\"type\":\"int\"}],\"schemaStringProp\":\"stringyMcStringface\",\"schemaNestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\"}";
    Schema schema = Schema.parse(TestUtil.load("RecordWithFieldProps.avsc"));
    String serialized = AvroCompatibilityHelper.getAvscWriter(AvscGenerationConfig.LEGACY_ONELINE, null).toAvsc(schema);
    Assert.assertEquals(serialized, expected);
  }

  @Test
  public void testLegacyOneLineWithPlugins() throws IOException {
    // Only string and serialized json props retained in avro 1.6
    // fields stringProp before nestedJsonProp as stringProp is being handled by plugin.
    String expected = "{\"type\":\"record\",\"name\":\"RecordWithFieldProps\",\"namespace\":\"com.acme\",\"doc\":\"A perfectly normal record with field props\",\"fields\":[{\"name\":\"stringField\",\"type\":\"string\",\"stringProp\":\"stringValue\",\"nestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\"},{\"name\":\"intField\",\"type\":\"int\"}],\"schemaNestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\",\"schemaStringProp\":\"stringyMcStringface\"}";
    Schema schema = Schema.parse(TestUtil.load("RecordWithFieldProps.avsc"));
    String serialized = AvroCompatibilityHelper.getAvscWriter(AvscGenerationConfig.LEGACY_ONELINE,
            Arrays.asList(
                new FieldLevelPlugin("stringProp"),
                new SchemaLevelPlugin("schemaNestedJsonProp")
            ))
        .toAvsc(schema);
    Assert.assertEquals(serialized, expected);
  }

  private class FieldLevelPlugin extends AvscWriterPlugin {

    public FieldLevelPlugin(String prop_name) {
      super(prop_name);
    }

    private void writeProp(String propName, Object prop, Jackson1JsonGeneratorWrapper gen){
      JsonGenerator delegate = gen.getDelegate();
      try {
        delegate.writeObjectField(propName, Jackson1Utils.toJsonNode(prop));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public String execute(Schema.Field field, JsonGeneratorWrapper gen) {
      Object prop = field.getProp(PROP_NAME);
      if (prop == null) {
        return null;
      }
      writeProp(PROP_NAME, prop, (Jackson1JsonGeneratorWrapper) gen);
      return PROP_NAME;
    }
  }

  private class SchemaLevelPlugin extends AvscWriterPlugin {
    public SchemaLevelPlugin(String prop_name) {
      super(prop_name);
    }

    private void writeProp(String propName, Object prop, Jackson1JsonGeneratorWrapper gen) {
      JsonGenerator delegate = gen.getDelegate();
      try {
        delegate.writeObjectField(propName, Jackson1Utils.toJsonNode(prop));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public String execute(Schema schema, JsonGeneratorWrapper gen) {
      String prop = schema.getProp(PROP_NAME);
      if (prop == null) {
        return null;
      }
      writeProp(PROP_NAME, prop, (Jackson1JsonGeneratorWrapper) gen);
      return PROP_NAME;
    }
  }
}
