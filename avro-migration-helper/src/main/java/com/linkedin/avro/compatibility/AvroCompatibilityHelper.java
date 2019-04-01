/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.compatibility;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Avro14Factory;
import org.apache.avro.io.Avro17Factory;
import org.apache.avro.io.AvroFactory;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.JsonGenerator;


/**
 * this is a helper class that's intended to work with either avro 1.4 or
 * a more modern version (1.7+) on the classpath.
 */
public class AvroCompatibilityHelper {
  private static final AvroVersion AVRO_VERSION;
  private static final AvroFactory FACTORY;

  static {
    AVRO_VERSION = detectAvroVersion();
    try {
      switch (AVRO_VERSION) {
        case AVRO_1_4:
          FACTORY = new Avro14Factory();
          break;
        case AVRO_1_7:
        case AVRO_1_8:
          FACTORY = new Avro17Factory();
          break;
        default:
          //1.5/1.6 support is out of scope for the migration effort and so are not supported (yet?)
          throw new IllegalStateException("unhandled avro version " + AVRO_VERSION);
      }
    } catch (Exception e) {
      throw new IllegalStateException("could not initialize avro factory for " + AVRO_VERSION, e);
    }
  }

  private AvroCompatibilityHelper() {
    //this is a util class. dont build one yourself
  }

  public static BinaryEncoder newBinaryEncoder(OutputStream out) {
    return FACTORY.newBinaryEncoder(out);
  }

  /**
   * to be migrated to SpecificData.getEncoder() in avro 1.8+
   * @param out object output
   * @return a binary encoder on top of the given ObjectOutput
   */
  public static BinaryEncoder newBinaryEncoder(ObjectOutput out) {
    return newBinaryEncoder(new ObjectOutputToOutputStreamAdapter(out));
  }

  /**
   * to be migrated to SpecificData.getDecoder() in avro 1.8+
   * @param in object input
   * @return a binary decoder on top of the given ObjectInput
   */
  public static BinaryDecoder newBinaryDecoder(ObjectInput in) {
    return DecoderFactory.defaultFactory().createBinaryDecoder(new ObjectInputToInputStreamAdapter(in), null);
  }

  public static JsonEncoder newJsonEncoder(Schema schema, OutputStream out) throws IOException {
    return FACTORY.newJsonEncoder(schema, out);
  }

  public static JsonEncoder newJsonEncoder(Schema schema, JsonGenerator jsonGenerator) throws IOException {
    return FACTORY.newJsonEncoder(schema, jsonGenerator);
  }

  public static JsonDecoder newJsonDecoder(Schema schema, InputStream input) throws IOException {
    return FACTORY.newJsonDecoder(schema, input);
  }

  public static JsonDecoder newJsonDecoder(Schema schema, String input) throws IOException {
    return FACTORY.newJsonDecoder(schema, input);
  }

  public static GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
    return FACTORY.newEnumSymbol(avroSchema, enumValue);
  }

  public static GenericData.Fixed newFixedField(Schema ofType) {
    return FACTORY.newFixedField(ofType);
  }

  public static GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
    return FACTORY.newFixedField(ofType, contents);
  }

  /**
   * Return true if the {@link IndexedRecord} is a {@link SpecificRecord}.
   *
   * This can be a source of confusion in avro 1.7.7+ because SpecificRecordBase implements GenericRecord
   * so it would be wrong to check for instanceof GenericRecord!
   *
   * @param indexedRecord the record in question
   * @return true if argument is a specific record
   */
  public static boolean isSpecificRecord(IndexedRecord indexedRecord) {
    return indexedRecord instanceof SpecificRecord;
  }

  /**
   * Return true if the {@link IndexedRecord} is a {@link org.apache.avro.generic.GenericRecord}.
   * @param indexedRecord a record
   * @return true if argument is a generic record
   */
  public static boolean isGenericRecord(IndexedRecord indexedRecord) {
    return !(isSpecificRecord(indexedRecord));
  }

  /**
   * generated java code (specific classes) for a given set of Schemas. this method fails if any failure occurs
   * during code generation
   * @param toCompile set of schema objects
   * @param compatibilityLevel desired target avro version for compatibility
   * @return a collection of generated java file descriptors, each with a relative path and proposed contents
   */
  public static Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion compatibilityLevel) {
    return FACTORY.compile(toCompile, compatibilityLevel);
  }

  public static AvroVersion getRuntimeAvroVersion() {
    return AVRO_VERSION;
  }

  public static Schema parse(String schemaJson) {
    return FACTORY.parse(schemaJson);
  }

  public static String toParsingForm(Schema s) {
    return FACTORY.toParsingForm(s);
  }

  private static AvroVersion detectAvroVersion() {

    //the following code looks for a series of changes made to the avro codebase for major
    //releases, and that have ideally remained "stable" ever since their introduction.

    //BinaryEncoder was made abstract for 1.5.0 as part of AVRO-753
    try {
      Class<?> binaryEncoderClass = Class.forName("org.apache.avro.io.BinaryEncoder");
      if (!Modifier.isAbstract(binaryEncoderClass.getModifiers())) {
        return AvroVersion.AVRO_1_4;
      }
    } catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException("unable to find class org.apache.avro.io.BinaryEncoder", unexpected);
    }

    //GenericData.StringType was added for 1.6.0 as part of AVRO-803
    try {
      Class.forName("org.apache.avro.generic.GenericData$StringType");
    } catch (ClassNotFoundException expected) {
      return AvroVersion.AVRO_1_5;
    }

    //SchemaNormalization was added for 1.7.0 as part of AVRO-1006
    try {
      Class.forName("org.apache.avro.SchemaNormalization");
    } catch (ClassNotFoundException expected) {
      return AvroVersion.AVRO_1_6;
    }

    //extra constructor added to EnumSymbol for 1.8.0 as part of AVRO-997
    try {
      Class<?> enumSymbolClass = Class.forName("org.apache.avro.generic.GenericData$EnumSymbol");
      enumSymbolClass.getConstructor(Schema.class, Object.class);
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_7;
    } catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException("unable to find class org.apache.avro.generic.GenericData$EnumSymbol", unexpected);
    }

    //method added for 1.9.0 as part of AVRO-2360
    try {
      Class<?> conversionClass = Class.forName("org.apache.avro.Conversion");
      conversionClass.getMethod("adjustAndSetValue", String.class, String.class);
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_8;
    } catch (ClassNotFoundException unexpected) {
      throw new IllegalStateException("unable to find class org.apache.avro.Conversion", unexpected);
    }

    return AvroVersion.AVRO_1_9;
  }
}
