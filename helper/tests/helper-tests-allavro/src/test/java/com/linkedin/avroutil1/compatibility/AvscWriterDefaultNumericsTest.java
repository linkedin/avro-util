package com.linkedin.avroutil1.compatibility;

import java.util.Locale;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.ValueMatcher;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.testng.annotations.Test;


public class AvscWriterDefaultNumericsTest {
  // Make sure the exact string values match (decimal points, trailing zeros, etc - i.e "-1" vs "-1.0").
  static CustomComparator exactDefaultNumericMatchComparator() {
    final ValueMatcher<Object> jsonStringValueMatcher = (actual, expected) -> actual.toString()
        .equals(expected.toString());

    return new CustomComparator(
        JSONCompareMode.NON_EXTENSIBLE,
        new Customization("fields[*].default", jsonStringValueMatcher));
  }


  @Test
  public void testWriteBadNumericDefaultValues() throws Exception {
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.INT, "-1.0", "-1", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.INT, "0.0", "0", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.INT, "1", "1", false);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.INT, "3.14159", "3.14159", false);

    runAvscWriterOnBadNumericDefaultValues(Schema.Type.LONG, "-1.0", "-1", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.LONG, "0.0", "0", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.LONG, "1", "1", false);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.LONG, "3.14159", "3.14159", false);

    runAvscWriterOnBadNumericDefaultValues(Schema.Type.FLOAT, "-1", "-1.0", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.FLOAT, "0", "0.0", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.FLOAT, "0.0", "0.0", false);

    runAvscWriterOnBadNumericDefaultValues(Schema.Type.DOUBLE, "-1", "-1.0", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.DOUBLE, "0", "0.0", true);
    runAvscWriterOnBadNumericDefaultValues(Schema.Type.DOUBLE, "0.0", "0.0", false);
  }

  @Test
  public void testWriteBadUnionDefaultValues() throws Exception {
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.INT, "-1", "-1", false);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.INT, "-1.0", "-1", false);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.INT, "0.0", "0", false);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.INT, "0", "0", false);

    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.LONG, "-1.0", "-1", true);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.LONG, "0.0", "0", true);

    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.FLOAT, "-1", "-1.0", true);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.FLOAT, "0", "0.0", true);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.FLOAT, "1.0", "1.0", false);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.FLOAT, "0.0", "0.0", false);

    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.DOUBLE, "1", "1.0", false);
    runAvscWriterOnBadUnionWithNumericDefaultValues(Schema.Type.DOUBLE, "0", "0.0", false);

  }

  public void runAvscWriterOnBadNumericDefaultValues(Type avroType, String defaultValue, String expectedDefaultValue, boolean allowFail) throws Exception {
    String avscTemplate = "{\"type\": \"record\", \"name\": \"HasBadDefaults\", \"fields\": "
        + "[{\"name\": \"age\", \"type\": \"%s\", \"default\":%s}]}";
    try {
      String expectedAvsc = String.format(avscTemplate, avroType.toString().toLowerCase(Locale.ROOT), expectedDefaultValue);

      String inputAvsc = String.format(avscTemplate, avroType.toString().toLowerCase(Locale.ROOT), defaultValue);
      Schema parsed = AvroCompatibilityHelper.parse(inputAvsc, SchemaParseConfiguration.LOOSE, null).getMainSchema();
      String processedAvsc = AvroCompatibilityHelper.toAvsc(parsed, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY);

      JSONAssert.assertEquals(expectedAvsc, processedAvsc, exactDefaultNumericMatchComparator());
    } catch (AvroTypeException expected) {
      if (allowFail && expected.getMessage().contains("Invalid default")) {
        return;
      }
      throw expected;
    }
  }

  public void runAvscWriterOnBadUnionWithNumericDefaultValues(Type avroType, String defaultValue, String expectedDefaultValue, boolean allowFail) throws Exception {
    String avscTemplate = "{\"type\": \"record\", \"name\": \"HasBadDefaults\", \"fields\": "
        + "[{\"name\": \"age\", \"type\": [\"%s\", \"null\"], \"default\":%s}]}";

    try {
      String expectedAvsc = String.format(avscTemplate, avroType.toString().toLowerCase(Locale.ROOT), expectedDefaultValue);

      String inputAvsc = String.format(avscTemplate, avroType.toString().toLowerCase(Locale.ROOT), defaultValue);
      Schema parsed = AvroCompatibilityHelper.parse(inputAvsc, SchemaParseConfiguration.LOOSE, null).getMainSchema();
      String processedAvsc = AvroCompatibilityHelper.toAvsc(parsed, AvscGenerationConfig.CORRECT_MITIGATED_PRETTY);
      JSONAssert.assertEquals(expectedAvsc, processedAvsc, exactDefaultNumericMatchComparator());
    } catch (AvroTypeException expected) {
      if (allowFail && expected.getMessage().contains("Invalid default")) {
        return;
      }
      throw expected;
    }
  }

}
