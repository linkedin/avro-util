/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import com.fasterxml.jackson.core.JsonGenerator;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.Jackson2JsonGeneratorWrapper;
import com.linkedin.avroutil1.compatibility.JsonGeneratorWrapper;
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import com.linkedin.avroutil1.testcommon.TestUtil;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.util.internal.JacksonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class Avro110AvscWriterTest {

  @Test
  public void testLegacyOneLine() throws IOException {
    // Avro 1.10 gets all props including int, null, boolean, object, string etc
    String expected = "{\"type\":\"record\",\"name\":\"RecordWithFieldProps\",\"namespace\":\"com.acme\",\"doc\":\"A perfectly normal record with field props\",\"fields\":[{\"name\":\"stringField\",\"type\":\"string\",\"nestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\",\"boolProp\":true,\"nullProp\":null,\"objectProp\":{\"a\":\"b\",\"c\":\"d\"},\"floatProp\":42.42,\"intProp\":42,\"stringProp\":\"stringValue\"},{\"name\":\"intField\",\"type\":\"int\"}],\"schemaStringProp\":\"stringyMcStringface\",\"schemaNullProp\":null,\"schemaBoolProp\":false,\"schemaIntProp\":24,\"schemaNestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\",\"schemaFloatProp\":1.2,\"schemaObjectProp\":{\"e\":\"f\",\"g\":\"h\"}}";
    Schema schema = Schema.parse(TestUtil.load("RecordWithFieldProps.avsc"));
    String serialized = AvroCompatibilityHelper.getAvscWriter(AvscGenerationConfig.LEGACY_ONELINE, null).toAvsc(schema);
    Assert.assertEquals(serialized, expected);
  }

  @Test
  public void testLegacyOneLineWithFieldPlugin() throws IOException {
    // Avro 1.10 gets all props including int, null, boolean, object, string etc
    // field's stringProp before nestedJsonProp as stringProp is being handled by plugin.
    // schema's schemaNestedJsonProp before other props as it is handled by plugin
    String expected =
        "{\"type\":\"record\",\"name\":\"RecordWithFieldProps\",\"namespace\":\"com.acme\",\"doc\":\"A perfectly normal record with field props\",\"fields\":[{\"name\":\"stringField\",\"type\":\"string\",\"stringProp\":\"stringValue\",\"nestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\",\"boolProp\":true,\"nullProp\":null,\"objectProp\":{\"a\":\"b\",\"c\":\"d\"},\"floatProp\":42.42,\"intProp\":42},{\"name\":\"intField\",\"type\":\"int\"}],\"schemaNestedJsonProp\":\"{\\\"innerKey\\\" : \\\"innerValue\\\"}\",\"schemaStringProp\":\"stringyMcStringface\",\"schemaNullProp\":null,\"schemaBoolProp\":false,\"schemaIntProp\":24,\"schemaFloatProp\":1.2,\"schemaObjectProp\":{\"e\":\"f\",\"g\":\"h\"}}" ;
    Schema schema = Schema.parse(TestUtil.load("RecordWithFieldProps.avsc"));
    String serialized = AvroCompatibilityHelper.getAvscWriter(AvscGenerationConfig.LEGACY_ONELINE,
            Arrays.asList(new FieldLevelPlugin("stringProp"), new SchemaLevelPlugin("schemaNestedJsonProp")))
        .toAvsc(schema);
    Assert.assertEquals(serialized, expected);
  }

  private class SchemaLevelPlugin extends AvscWriterPlugin {
    public SchemaLevelPlugin(String prop_name) {
      super(prop_name);
    }

    private void writeProp(String propName, Object prop, Jackson2JsonGeneratorWrapper gen){
      JsonGenerator delegate = gen.getDelegate();
      try {
        delegate.writeObjectField(propName, JacksonUtils.toJsonNode(prop));
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    @Override
    public String execute(Schema schema, JsonGeneratorWrapper gen) {
      if (schema.hasProps()) {
        String prop = schema.getProp(PROP_NAME);
        if(prop == null) {
          return null;
        }
        writeProp(PROP_NAME, prop, (Jackson2JsonGeneratorWrapper) gen);
      }
      return PROP_NAME;
    }
  }

  private class FieldLevelPlugin extends AvscWriterPlugin {

    public FieldLevelPlugin(String prop_name) {
      super(prop_name);
    }

    private void writeProp(String propName, Object prop, Jackson2JsonGeneratorWrapper gen){
      JsonGenerator delegate = gen.getDelegate();
      try {
        delegate.writeObjectField(propName, JacksonUtils.toJsonNode(prop));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public String execute(Schema.Field field, JsonGeneratorWrapper gen) {
      if (field.hasProps()) {
        Object prop = field.getObjectProp(PROP_NAME);
        if(prop == null) {
          return null;
        }
        writeProp(PROP_NAME, prop, (Jackson2JsonGeneratorWrapper) gen);
      }
      return PROP_NAME;
    }
  }
}
