/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


/**
 * tests the declared-schema-related methods on the {@link AvroCompatibilityHelper} class
 */
public class AvroSchemaUtilTest {

  @Test
  public void testGetDeclaredSchemaOnSpecificEnumClasses() throws Exception {
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "under" + suffix + ".SimpleEnum";
      Class<?> enumClass = Class.forName(fullname);
      Assert.assertNotNull(enumClass);
      Schema schema = AvroSchemaUtil.getDeclaredSchema(enumClass);
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.ENUM);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnVanillaSpecificEnumClasses() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "by" + suffix + ".SimpleEnum";
      Class<?> enumClass = null;
      try {
        enumClass = Class.forName(fullname);
      } catch (NoClassDefFoundError maybeExpected) {
        //this indicates something other (worse) than the immediate class not being found
        //usually its some required class not being found
        //noinspection SwitchStatementWithTooFewBranches
        switch (runtimeAvroVersion) {
          case AVRO_1_4:
            if (supportedVersion.laterThan(AvroVersion.AVRO_1_5)) {
              //classes generated by vanilla avro 1.6+ use class Schema.Parser in SCHEMA$ declaration,
              //which does not exist under 1.4, causing a classloading error
              continue;
            }
          default:
            break;
        }
        Assert.fail("unexpected failure loading " + fullname + " under " + runtimeAvroVersion, maybeExpected);
      }
      Assert.assertNotNull(enumClass);
      Schema schema = AvroSchemaUtil.getDeclaredSchema(enumClass);
      if (supportedVersion.earlierThan(AvroVersion.AVRO_1_5)) {
        //enum classes generated by vanilla avro 1.4 have no SCHEMA$
        Assert.assertNull(schema);
        continue;
      }
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.ENUM);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnSpecificEnumInstance() throws Exception {
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "under" + suffix + ".SimpleEnum";
      @SuppressWarnings("unchecked")
      Class<? extends Enum<?>> enumClass = (Class<? extends Enum<?>>) Class.forName(fullname);
      Assert.assertNotNull(enumClass);
      Enum<?> symbolA = (Enum<?>) enumClass.getField("A").get(null);
      Assert.assertNotNull(symbolA);
      Schema schema = AvroSchemaUtil.getDeclaredSchema(symbolA);
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.ENUM);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnVanillaSpecificEnumInstance() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "by" + suffix + ".SimpleEnum";
      Class<? extends Enum<?>> enumClass = null;
      try {
        //noinspection unchecked
        enumClass = (Class<? extends Enum<?>>) Class.forName(fullname);
      } catch (NoClassDefFoundError maybeExpected) {
        if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_5) && supportedVersion.laterThan(AvroVersion.AVRO_1_5)) {
          //code generated by vanilla avro 1.6+ expects Schema.Parser class, which does not exist under 1.4
          continue;
        }
      }
      Assert.assertNotNull(enumClass, "evaluating " + fullname + " under " + runtimeAvroVersion);
      Enum<?> symbolA = (Enum<?>) enumClass.getField("A").get(null);
      Assert.assertNotNull(symbolA);
      Schema schema = AvroSchemaUtil.getDeclaredSchema(symbolA);

      if (supportedVersion.earlierThan(AvroVersion.AVRO_1_5)) {
        //cant get schema from code generated by 1.4
        Assert.assertNull(schema);
        continue;
      }

      Assert.assertNotNull(schema, "evaluating " + fullname + " under " + runtimeAvroVersion);
      Assert.assertEquals(schema.getType(), Schema.Type.ENUM);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnGenericEnumInstance() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    //doing this for all avro versions is overkill, but ~harmless
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "under" + suffix + ".SimpleEnum";
      @SuppressWarnings("unchecked")
      Class<? extends Enum<?>> enumClass = (Class<? extends Enum<?>>) Class.forName(fullname);
      Assert.assertNotNull(enumClass);
      Schema fromClass = AvroSchemaUtil.getDeclaredSchema(enumClass);
      GenericData.EnumSymbol symbolA = AvroCompatibilityHelper.newEnumSymbol(fromClass, "A", true);
      Schema schema = AvroSchemaUtil.getDeclaredSchema(symbolA);
      if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_5)) {
        Assert.assertNull(schema); //cant get schema out of generic enum symbols under 1.4
        continue;
      }
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.ENUM);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnSpecificFixedClasses() throws Exception {
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "under" + suffix + ".SimpleFixed";
      Class<?> enumClass = Class.forName(fullname);
      Assert.assertNotNull(enumClass);
      Schema schema = AvroSchemaUtil.getDeclaredSchema(enumClass);
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.FIXED);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnVanillaSpecificFixedClasses() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "by" + suffix + ".SimpleFixed";
      Class<?> fixedClass = null;
      try {
        fixedClass = Class.forName(fullname);
      } catch (NoClassDefFoundError maybeExpected) {
        //this indicates something other (worse) than the immediate class not being found
        //usually its some required class not being found
        //noinspection SwitchStatementWithTooFewBranches
        switch (runtimeAvroVersion) {
          case AVRO_1_4:
            if (supportedVersion.laterThan(AvroVersion.AVRO_1_5)) {
              //classes generated by vanilla avro 1.6+ use class Schema.Parser in SCHEMA$ declaration,
              //which does not exist under 1.4, causing a classloading error
              continue;
            }
          default:
            break;
        }
        Assert.fail("unexpected failure loading " + fullname + " under " + runtimeAvroVersion, maybeExpected);
      }
      Assert.assertNotNull(fixedClass);
      Schema schema = AvroSchemaUtil.getDeclaredSchema(fixedClass);
      if (supportedVersion.earlierThan(AvroVersion.AVRO_1_5)) {
        //fixed classes generated by vanilla avro 1.4 have no SCHEMA$
        Assert.assertNull(schema);
        continue;
      }
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.FIXED);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnSpecificFixedInstance() throws Exception {
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "under" + suffix + ".SimpleFixed";
      Class<?> fixedClass = Class.forName(fullname);
      Assert.assertNotNull(fixedClass);

      SpecificFixed instance = (SpecificFixed) fixedClass.newInstance();

      Schema schema = AvroSchemaUtil.getDeclaredSchema(instance);
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.FIXED);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnVanillaSpecificFixedInstance() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "by" + suffix + ".SimpleFixed";
      Class<?> fixedClass = null;
      try {
        fixedClass = Class.forName(fullname);
      } catch (NoClassDefFoundError maybeExpected) {
        //this indicates something other (worse) than the immediate class not being found
        //usually its some required class not being found
        //noinspection SwitchStatementWithTooFewBranches
        switch (runtimeAvroVersion) {
          case AVRO_1_4:
            if (supportedVersion.laterThan(AvroVersion.AVRO_1_5)) {
              //classes generated by vanilla avro 1.6+ use class Schema.Parser in SCHEMA$ declaration,
              //which does not exist under 1.4, causing a classloading error
              continue;
            }
          default:
            break;
        }
        Assert.fail("unexpected failure loading " + fullname + " under " + runtimeAvroVersion, maybeExpected);
      }
      Assert.assertNotNull(fixedClass);

      SpecificFixed instance = null;
      try {
        instance = (SpecificFixed) fixedClass.newInstance();
      } catch (AbstractMethodError maybeExpected) {
        if (supportedVersion.earlierThan(AvroVersion.AVRO_1_8) && runtimeAvroVersion.laterThan(AvroVersion.AVRO_1_7)) {
          //starting with avro 1.8 specific fixed classes implement GenericContainer and need to have a getSchema()
          //instance method - which code generated by vanilla older avro does not have (this is called at ctr time)
          continue;
        }
        Assert.fail("unexpected failure loading " + fullname + " under " + runtimeAvroVersion, maybeExpected);
      } catch (AvroRuntimeException maybeExpected) {
        if (supportedVersion.earlierThan(AvroVersion.AVRO_1_5) && runtimeAvroVersion.laterThan(AvroVersion.AVRO_1_4)) {
          //SpecificFixed in avro 1.5+ expected a SCHEMA$ field, which classes generated by vanilla 1.4 do not have
          continue;
        }
        Assert.fail("unexpected failure loading " + fullname + " under " + runtimeAvroVersion, maybeExpected);
      }

      Schema schema = AvroSchemaUtil.getDeclaredSchema(instance);

      if (supportedVersion.earlierThan(AvroVersion.AVRO_1_5)) {
        //fixed classes generated by vanilla 1.4 dont have SCHEMA$
        Assert.assertNull(schema);
        continue;
      }
      Assert.assertNotNull(schema);
      Assert.assertEquals(schema.getType(), Schema.Type.FIXED);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnGenericFixedInstance() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "under" + suffix + ".SimpleFixed";
      Class<?> fixedClass = Class.forName(fullname);
      Assert.assertNotNull(fixedClass);
      Schema fromClass = AvroSchemaUtil.getDeclaredSchema(fixedClass);
      Assert.assertNotNull(fromClass);

      GenericFixed instance = AvroCompatibilityHelper.newFixed(fromClass);

      Schema schema = AvroSchemaUtil.getDeclaredSchema(instance);

      if (runtimeAvroVersion.earlierThan(AvroVersion.AVRO_1_5)) {
        //cant get schema from generic fixed under 1.4
        Assert.assertNull(schema);
        continue;
      }

      Assert.assertNotNull(schema, "unexpected failure loading " + fullname + " under " + runtimeAvroVersion);
      Assert.assertEquals(schema.getType(), Schema.Type.FIXED);
      Assert.assertEquals(schema.getFullName(), fullname);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnSpecificRecordClassesAndInstances() throws Exception {
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "under" + suffix + ".SimpleRecord";
      Class<?> recordClass = Class.forName(fullname);
      Assert.assertNotNull(recordClass);
      Schema fromClass = AvroSchemaUtil.getDeclaredSchema(recordClass);
      Assert.assertNotNull(fromClass);
      Assert.assertEquals(fromClass.getType(), Schema.Type.RECORD);
      Assert.assertEquals(fromClass.getFullName(), fullname);

      SpecificRecord instance = (SpecificRecord) recordClass.newInstance();
      Schema fromRecord = AvroSchemaUtil.getDeclaredSchema(instance);
      Assert.assertSame(fromRecord, fromClass);
    }
  }

  @Test
  public void testGetDeclaredSchemaOnVanillaSpecificRecordClassesAndInstances() throws Exception {
    AvroVersion runtimeAvroVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    for (AvroVersion supportedVersion : AvroVersion.values()) {
      String suffix = supportedVersion.name().replaceAll("[^\\d]+", "");
      String fullname = "by" + suffix + ".SimpleRecord";
      Class<?> recordClass = null;
      try {
        recordClass = Class.forName(fullname);
      } catch (NoClassDefFoundError | IllegalAccessError maybeExpected) {
        //this indicates something other (worse) than the immediate class not being found
        //usually its some required class not being found
        switch (runtimeAvroVersion) {
          case AVRO_1_4:
            if (supportedVersion.laterThan(AvroVersion.AVRO_1_5)) {
              //classes generated by vanilla avro 1.6+ use class Schema.Parser in SCHEMA$ declaration,
              //which does not exist under 1.4, causing a classloading error
              continue;
            }
          case AVRO_1_5:
          case AVRO_1_6:
            if (supportedVersion.laterThan(AvroVersion.AVRO_1_7)) {
              //classes generated by vanilla avro 1.8+ try and create an instance of org.apache.avro.specific.SpecificData
              //for field MODEL$. the constructor in question only exists under avro 1.7+
              continue;
            }
          case AVRO_1_7:
            if (supportedVersion.laterThan(AvroVersion.AVRO_1_7)) {
              //classes generated by vanilla avro 1.8+ try and create an instance of org.apache.avro.message.BinaryMessageEncoder
              //for field ENCODER (and decoder likewise). the class in question only exists under avro 1.8+
              continue;
            }
          default:
            break;
        }
        Assert.fail("unexpected failure loading " + fullname + " under " + runtimeAvroVersion, maybeExpected);
      }
      Assert.assertNotNull(recordClass);
      Schema fromClass = AvroSchemaUtil.getDeclaredSchema(recordClass);
      Assert.assertNotNull(fromClass, "operating on " + fullname + " under " + runtimeAvroVersion);
      Assert.assertEquals(fromClass.getType(), Schema.Type.RECORD);
      Assert.assertEquals(fromClass.getFullName(), fullname);

      SpecificRecord instance = (SpecificRecord) recordClass.newInstance();
      Schema fromRecord = AvroSchemaUtil.getDeclaredSchema(instance);
      Assert.assertSame(fromRecord, fromClass);
    }
  }

  @Test
  public void testSimpleExactResolution() throws Exception {
    for (Schema.Type writerType : HelperConsts.PRIMITIVE_TYPES) {
      Schema writerSchema = Schema.create(writerType);
      for (Schema.Type readerType : HelperConsts.PRIMITIVE_TYPES) {
        Schema readerSchema = Schema.create(readerType);

        SchemaResolutionResult result = AvroSchemaUtil.resolveReaderVsWriter(writerSchema, readerSchema, false, false);
        if (writerType == readerType) {
          Assert.assertNotNull(result, "reader " + readerSchema + " writer " + writerSchema);
          Assert.assertEquals(result.getReaderMatch(), readerSchema, "reader " + readerSchema + " writer " + writerSchema);
          Assert.assertEquals(result.getWriterMatch(), writerSchema, "reader " + readerSchema + " writer " + writerSchema);
        } else {
          Assert.assertNull(result, "reader " + readerSchema + " writer " + writerSchema);
        }
      }
    }
  }

  @Test
  public void testSimplePromotions() throws Exception {
    //int is promotable to long, float, or double
    assertPrimitivePromotions(Schema.Type.INT, Arrays.asList(Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE));
    //long is promotable to float or double
    assertPrimitivePromotions(Schema.Type.LONG, Arrays.asList(Schema.Type.FLOAT, Schema.Type.DOUBLE));
    //float is promotable to double
    assertPrimitivePromotions(Schema.Type.FLOAT, Collections.singletonList(Schema.Type.DOUBLE));
    //string is promotable to bytes
    assertPrimitivePromotions(Schema.Type.STRING, Collections.singletonList(Schema.Type.BYTES));
    //bytes is promotable to string
    assertPrimitivePromotions(Schema.Type.BYTES, Collections.singletonList(Schema.Type.STRING));
  }

  @Test
  public void testIsSpecific() throws Exception {
    //primitives
    Assert.assertNull(AvroSchemaUtil.isSpecific(null));
    Assert.assertNull(AvroSchemaUtil.isSpecific("a string"));
    Assert.assertNull(AvroSchemaUtil.isSpecific(42));
    Assert.assertNull(AvroSchemaUtil.isSpecific(Boolean.FALSE));

    //generics
    Assert.assertFalse(AvroSchemaUtil.isSpecific(AvroCompatibilityHelper.newEnumSymbol(under14.SimpleEnum.SCHEMA$, "B")));
    Assert.assertFalse(AvroSchemaUtil.isSpecific(AvroCompatibilityHelper.newEnumSymbol(under111.SimpleEnum.SCHEMA$, "B")));
    Assert.assertFalse(AvroSchemaUtil.isSpecific(AvroCompatibilityHelper.newFixed(under14.SimpleFixed.SCHEMA$, new byte[] {1, 2, 3})));
    Assert.assertFalse(AvroSchemaUtil.isSpecific(AvroCompatibilityHelper.newFixed(under111.SimpleFixed.SCHEMA$, new byte[] {1, 2, 3})));
    Assert.assertFalse(AvroSchemaUtil.isSpecific(new GenericData.Record(under14.SimpleRecord.SCHEMA$)));
    Assert.assertFalse(AvroSchemaUtil.isSpecific(new GenericData.Record(under111.SimpleRecord.SCHEMA$)));

    //specifics
    Assert.assertTrue(AvroSchemaUtil.isSpecific(under14.SimpleEnum.B));
    Assert.assertTrue(AvroSchemaUtil.isSpecific(under111.SimpleEnum.B));
    Assert.assertTrue(AvroSchemaUtil.isSpecific(new under14.SimpleFixed(new byte[] {1, 2, 3})));
    Assert.assertTrue(AvroSchemaUtil.isSpecific(new under111.SimpleFixed(new byte[] {1, 2, 3})));
    Assert.assertTrue(AvroSchemaUtil.isSpecific(new under14.SimpleRecord()));
    Assert.assertTrue(AvroSchemaUtil.isSpecific(new under111.SimpleRecord()));

    //collections of generics
    Assert.assertFalse(AvroSchemaUtil.isSpecific(
            Collections.singletonList(AvroCompatibilityHelper.newEnumSymbol(under14.SimpleEnum.SCHEMA$, "B")))
    );
    Map<String, Object> map = new HashMap<>();
    map.put("whatever", new GenericData.Record(under111.SimpleRecord.SCHEMA$));
    Assert.assertFalse(AvroSchemaUtil.isSpecific(map));

    //collections of specifics

    //schema doesnt need to match, just be array
    Collection<Object> col = new GenericData.Array<>(1, Schema.parse("{\"type\": \"array\", \"items\" : \"string\"}"));
    col.add(new under111.SimpleRecord());
    Assert.assertTrue(AvroSchemaUtil.isSpecific(col));
  }

  private void assertPrimitivePromotions(Schema.Type writerType, Collection<Schema.Type> promotableTo) {
    Schema writerSchema = Schema.create(writerType);
    for (Schema.Type readerType : promotableTo) {
      Schema readerSchema = Schema.create(readerType);
      SchemaResolutionResult result = AvroSchemaUtil.resolveReaderVsWriter(writerSchema, readerSchema, false, true);
      Assert.assertNotNull(result, "reader " + readerSchema + " writer " + writerSchema);
      Assert.assertEquals(result.getReaderMatch(), readerSchema, "reader " + readerSchema + " writer " + writerSchema);
      Assert.assertEquals(result.getWriterMatch(), writerSchema, "reader " + readerSchema + " writer " + writerSchema);
    }
    //but not to other primitives
    for (Schema.Type readerType : HelperConsts.PRIMITIVE_TYPES) {
      if (readerType == writerSchema.getType() || promotableTo.contains(readerType)) {
        continue;
      }
      Schema readerSchema = Schema.create(readerType);
      SchemaResolutionResult result = AvroSchemaUtil.resolveReaderVsWriter(writerSchema, readerSchema, false, true);
      Assert.assertNull(result, "reader " + readerSchema + " writer " + writerSchema);
    }
  }
}
