/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.apache.avro.specific.SpecificData;


public class AvroCompatibilityHelperCommon {
  protected static final AvroVersion DETECTED_VERSION;

  static {
    DETECTED_VERSION = detectAvroVersion();
  }

  /**
   * detects the version of avro available on the runtime classpath
   * @return the avro version on the runtime classpath, or null if no avro found at all
   */
  private static AvroVersion detectAvroVersion() {

    //the following code looks for a series of changes made to the avro codebase for major
    //releases, and that have ideally remained "stable" ever since their introduction.

    Class<?> schemaClass;
    try {
      schemaClass = Class.forName("org.apache.avro.Schema");
      VersionDetectionUtil.markUsedForCoreAvro(schemaClass);
    } catch (ClassNotFoundException unexpected) {
      return null; //no avro on the classpath at all
    }

    //BinaryEncoder was made abstract for 1.5.0 as part of AVRO-753
    try {
      Class<?> binaryEncoderClass = Class.forName("org.apache.avro.io.BinaryEncoder");
      VersionDetectionUtil.markUsedForCoreAvro(binaryEncoderClass);
      if (!Modifier.isAbstract(binaryEncoderClass.getModifiers())) {
        return AvroVersion.AVRO_1_4;
      }
    } catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException("unable to find class org.apache.avro.io.BinaryEncoder", unexpected);
    }

    //GenericData.StringType was added for 1.6.0 as part of AVRO-803
    try {
      Class<?> StringTypeClass = Class.forName("org.apache.avro.generic.GenericData$StringType");
      VersionDetectionUtil.markUsedForCoreAvro(StringTypeClass);
    } catch (ClassNotFoundException expected) {
      return AvroVersion.AVRO_1_5;
    }

    //SchemaNormalization was added for 1.7.0 as part of AVRO-1006
    try {
      Class<?> schemaNormalizationClass = Class.forName("org.apache.avro.SchemaNormalization");
      VersionDetectionUtil.markUsedForCoreAvro(schemaNormalizationClass);
    } catch (ClassNotFoundException expected) {
      return AvroVersion.AVRO_1_6;
    }

    //extra constructor added to EnumSymbol for 1.8.0 as part of AVRO-997
    try {
      Class<?> enumSymbolClass = Class.forName("org.apache.avro.generic.GenericData$EnumSymbol");
      VersionDetectionUtil.markUsedForCoreAvro(enumSymbolClass);
      enumSymbolClass.getConstructor(schemaClass, Object.class);
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_7;
    } catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException("unable to find class org.apache.avro.generic.GenericData$EnumSymbol", unexpected);
    }

    //method added for 1.9.0 as part of AVRO-2360
    try {
      Class<?> conversionClass = Class.forName("org.apache.avro.Conversion");
      VersionDetectionUtil.markUsedForCoreAvro(conversionClass);
      conversionClass.getMethod("adjustAndSetValue", String.class, String.class);
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_8;
    } catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException("unable to find class org.apache.avro.Conversion", unexpected);
    }

    //method added for 1.10.0 as part of AVRO-2822
    try {
      //noinspection JavaReflectionMemberAccess
      schemaClass.getMethod("toString", Collection.class, Boolean.TYPE);
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_9;
    }

    // DataFileReader(SeekableInput sin, DatumReader<D> reader, boolean closeOnError, byte[] magic)
    // was introduced in 1.11.1 https://issues.apache.org/jira/browse/AVRO-3482
    boolean dataFileReaderClassContains1_11Constructor = false;
    try {
      Class<?> dataFileReaderClass = Class.forName("org.apache.avro.file.DataFileReader");
      Constructor<?>[] constructors = dataFileReaderClass.getDeclaredConstructors();
      for (Constructor<?> constructor : constructors) {
        if (constructor.toString().equals(
            "protected org.apache.avro.file.DataFileReader(" +
                "org.apache.avro.file.SeekableInput," +
                "org.apache.avro.io.DatumReader," +
                "boolean,byte[]) " +
                "throws java.io.IOException")) {
          dataFileReaderClassContains1_11Constructor = true;
          break;
        }
      }
    } catch (ClassNotFoundException ignored) {
      //empty
    }

    try {
      //public static final Set<String> RESERVED_WORDS exists in 1.8+
      @SuppressWarnings("JavaReflectionMemberAccess")
      Field reservedWordsField = SpecificData.class.getField("RESERVED_WORDS");
      @SuppressWarnings("unchecked")
      Collection<String> reservedWords = (Collection<String>) reservedWordsField.get(null);
      //"record" was added as a reserved word in 1.11.0 (but removed from 1.11.1+)
      // as part of https://issues.apache.org/jira/browse/AVRO-3116
      // To check if version 1.11.1+, we check for presence of the new DataFileReader constructor.
      if (!reservedWords.contains("record") && !dataFileReaderClassContains1_11Constructor) {
        return AvroVersion.AVRO_1_10;
      }
    } catch (NoSuchFieldException unexpected1) {
      throw new IllegalStateException("unable to find org.apache.avro.specific.SpecificData.RESERVED_WORDS", unexpected1);
    } catch (IllegalAccessException unexpected2) {
      throw new IllegalStateException("unable to access org.apache.avro.specific.SpecificData.RESERVED_WORDS", unexpected2);
    }

    return AvroVersion.AVRO_1_11;
  }

  /**
   * returns the detected runtime version of avro, or null if none found
   * @return the version of avro detected on the runtime classpath, or null if no avro found
   */
  public static AvroVersion getRuntimeAvroVersion() {
    return DETECTED_VERSION;
  }
}
