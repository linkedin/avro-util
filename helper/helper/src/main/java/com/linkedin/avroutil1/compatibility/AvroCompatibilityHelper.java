/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.compatibility.avro110.Avro110Adapter;
import com.linkedin.avroutil1.compatibility.avro111.Avro111Adapter;
import com.linkedin.avroutil1.compatibility.avro14.Avro14Adapter;
import com.linkedin.avroutil1.compatibility.avro15.Avro15Adapter;
import com.linkedin.avroutil1.compatibility.avro16.Avro16Adapter;
import com.linkedin.avroutil1.compatibility.avro17.Avro17Adapter;
import com.linkedin.avroutil1.compatibility.avro18.Avro18Adapter;
import com.linkedin.avroutil1.compatibility.avro19.Avro19Adapter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;


/**
 * a Utility class for performing various avro-related operations under a wide range of avro versions at runtime.
 */
public class AvroCompatibilityHelper  extends AvroCompatibilityHelperCommon{
  private static volatile AvroVersion DETECTED_COMPILER_VERSION;
  private static volatile Exception COMPILER_DETECTION_ISSUE;
  private static final AvroAdapter ADAPTER;

  static {
    //noinspection ConstantConditions
    if (!AvroCompatibilityHelper.class.getCanonicalName().equals(HelperConsts.HELPER_FQCN)) {
      //protect against partial refactors
      throw new IllegalStateException("helper fqcn (" + AvroCompatibilityHelper.class.getCanonicalName() + ")"
          + " differs from constant (" + HelperConsts.HELPER_FQCN + ")");
    }

    try {
      DETECTED_COMPILER_VERSION = detectAvroCompilerVersion();
      COMPILER_DETECTION_ISSUE = null;
    } catch (Exception compilerDetectionIssue) {
      DETECTED_COMPILER_VERSION = null;
      COMPILER_DETECTION_ISSUE = compilerDetectionIssue;
    }

    if (DETECTED_VERSION == null) {
      ADAPTER = null;
    } else {
      try {
        switch (DETECTED_VERSION) {
          case AVRO_1_4:
            ADAPTER = new Avro14Adapter();
            break;
          case AVRO_1_5:
            ADAPTER = new Avro15Adapter();
            break;
          case AVRO_1_6:
            ADAPTER = new Avro16Adapter();
            break;
          case AVRO_1_7:
            ADAPTER = new Avro17Adapter();
            break;
          case AVRO_1_8:
            ADAPTER = new Avro18Adapter();
            break;
          case AVRO_1_9:
            ADAPTER = new Avro19Adapter();
            break;
          case AVRO_1_10:
            ADAPTER = new Avro110Adapter();
            break;
          case AVRO_1_11:
            ADAPTER = new Avro111Adapter();
            break;
          default:
            throw new IllegalStateException("unhandled avro version " + DETECTED_VERSION);
        }
      } catch (Throwable t) {
        throw new IllegalStateException("could not initialize avro factory for " + DETECTED_VERSION, t);
      }
    }
  }

  /**
   * @Deprecated: Use AvroCompatibilityHelperCommon.getRuntimeAvroVersion()
   *
   * returns the detected runtime version of avro, or null if none found
   * @return the version of avro detected on the runtime classpath, or null if no avro found
   */
  @Deprecated
  public static AvroVersion getRuntimeAvroVersion() {
    return DETECTED_VERSION;
  }

  /**
   * returns the detected runtime version of avro-compiler, or null if if no avro-compiler found
   * @return the version of avro-compiler detected on the runtime classpath, or null if no avro-compiler found
   */
  public static AvroVersion getRuntimeAvroCompilerVersion() {
    assertNoCompilerDetectionIssues();
    return DETECTED_COMPILER_VERSION;
  }

  // encoders/decoders

  /**
   * constructs a {@link BinaryEncoder} on top of the given output stream
   * @param out an output stream
   * @param buffered true for buffered encoder (when supported by runtime version of avro). these perform better
   * @param reuse a given encoder to reuse, if supported by the runtime avro
   * @return a {@link BinaryEncoder}
   */
  public static BinaryEncoder newBinaryEncoder(OutputStream out, boolean buffered, BinaryEncoder reuse) {
    assertAvroAvailable();
    return ADAPTER.newBinaryEncoder(out, buffered, reuse);
  }

  /**
   * a convenience method for creating a (buffered) {@link BinaryEncoder} on top of the given output stream. <br>
   * remember to flush.
   * @param out an output stream
   * @return a binary encoder on top of the given stream
   */
  public static BinaryEncoder newBinaryEncoder(OutputStream out) {
    return newBinaryEncoder(out, true, null);
  }

  /**
   * constructs a {@link BinaryEncoder} on top of the given {@link ObjectOutput}.
   * this is mostly meant as a runtime utility for generated classes that implement {@link java.io.Externalizable}
   * <br>
   * this method is mostly called from generated classes, <b>YOU PROBABLY DONT WANT TO USE THIS</b>
   * @param out an {@link ObjectOutput}
   * @return a {@link BinaryEncoder}
   */
  public static BinaryEncoder newBinaryEncoder(ObjectOutput out) {
    assertAvroAvailable();
    return ADAPTER.newBinaryEncoder(out);
  }

  /**
   * constructs a {@link BinaryDecoder} on top of the given input stream
   * @param in an input stream
   * @param buffered true for buffered decoder (if/when supported by runtime avro)
   * @param reuse a given decoder to reuse, if supported by the runtime avro
   * @return a {@link BinaryDecoder} for the given input stream, possibly reused
   */
  public static BinaryDecoder newBinaryDecoder(InputStream in, boolean buffered, BinaryDecoder reuse) {
    assertAvroAvailable();
    return ADAPTER.newBinaryDecoder(in, buffered, reuse);
  }

  /**
   * constructs or reinitializes a {@link BinaryDecoder} with the byte array
   * provided as the source of data.
   * @param bytes The byte array to initialize to
   * @param offset The offset to start reading from
   * @param length The maximum number of bytes to read from the byte array
   * @param reuse The BinaryDecoder to attempt to reinitialize.
   * @return A BinaryDecoder that uses <i>bytes</i> as its source of data. If
   * <i>reuse</i> is null, this will be a new instance.
   */
  public static BinaryDecoder newBinaryDecoder(byte[] bytes, int offset,
      int length, BinaryDecoder reuse) {
    assertAvroAvailable();
    return ADAPTER.newBinaryDecoder(bytes, offset, length, reuse);
  }

  /**
   * convenience method for getting a {@link BinaryDecoder} for a given byte[]
   * @param in byte array with data
   * @return a {@link BinaryDecoder} for decoding the given array
   */
  public static BinaryDecoder newBinaryDecoder(byte[] in) {
    return newBinaryDecoder(new ByteArrayInputStream(in), false, null);
  }

  /**
   * convenience method for getting a (buffered) {@link BinaryDecoder} for a given {@link InputStream}
   * @param in an input stream
   * @return a {@link BinaryDecoder} for decoding the given input stream
   */
  public static BinaryDecoder newBinaryDecoder(InputStream in) {
    return newBinaryDecoder(in, true, null);
  }

  /**
   * constructs a {@link BinaryDecoder} on top of the given {@link ObjectInput}.
   * this is mostly meant as a runtime utility for generated classes that implement {@link java.io.Externalizable}
   * <br>
   * this method is mostly called from generated classes, <b>YOU PROBABLY DONT WANT TO USE THIS</b>
   * @param in an {@link ObjectInput}
   * @return a {@link BinaryDecoder}
   */
  public static BinaryDecoder newBinaryDecoder(ObjectInput in) {
    assertAvroAvailable();
    return ADAPTER.newBinaryDecoder(in);
  }

  /**
   * constructs a {@link JsonEncoder} on top of the given {@link OutputStream} for the given {@link Schema}
   * @param schema a schema
   * @param out an output stream
   * @param pretty true to pretty-print the json (if supported by runtime avro version)
   * @return an encoder
   * @throws IOException in io errors
   */
  public static JsonEncoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newJsonEncoder(schema, out, pretty);
  }

  /**
   * constructs a json {@link Encoder} on top of the given {@link OutputStream} for the given {@link Schema}
   * @param schema a schema
   * @param out an output stream
   * @param pretty true to pretty-print the json (if supported by runtime avro version)
   * @param jsonFormat which major version of avro to match for json wire format. null means the runtime version
   * @return an encoder
   * @throws IOException in io errors
   */
  public static Encoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty, AvroVersion jsonFormat) throws IOException {
    assertAvroAvailable();
    //for some reason uncommenting this check causes classloading to fail if no avro
    //(see AvroCompatibilityHelperNoAvroTest).
    //if (jsonFormat == null) {
    //  return ADAPTER.newJsonEncoder(schema, out, pretty);
    //}
    return ADAPTER.newJsonEncoder(schema, out, pretty, jsonFormat);
  }

  /**
   * constructs a {@link JsonDecoder} on top of the given {@link InputStream} for the given {@link Schema}
   * @param schema a schema
   * @param in an input stream
   * @return a decoder
   * @throws IOException on io errors
   */
  public static JsonDecoder newJsonDecoder(Schema schema, InputStream in) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newJsonDecoder(schema, in);
  }

  /**
   * constructs a {@link JsonDecoder} on top of the given {@link String} for the given {@link Schema}
   * @param schema a schema
   * @param in a String containing a json-serialized avro payload
   * @return a decoder
   * @throws IOException on io errors
   */
  public static JsonDecoder newJsonDecoder(Schema schema, String in) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newJsonDecoder(schema, in);
  }

  /**
   * constructs a {@link JsonDecoder} on top of the given {@link InputStream} for the given {@link Schema}
   * that is more widely compatible than the "native" avro decoder:
   * <ul>
   *     <li>avro json format has changed between 1.4 and 1.5 around encoding of union branches - simple name vs full names for named types</li>
   *     <li>avro json decoders are not tolerant of int literals in json for float fields and vice versa under avro &lt; 1.7</li>
   * </ul>
   * the decoder returned by this method is expected to handle these cases with no errors
   * @param schema a schema
   * @param in an input stream containing a json-serialized avro payload
   * @return a decoder
   * @throws IOException on io errors
   */
  public static Decoder newCompatibleJsonDecoder(Schema schema, InputStream in) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newCompatibleJsonDecoder(schema, in);
  }

  /**
   * constructs a {@link JsonDecoder} on top of the given {@link String} for the given {@link Schema}
   * that can decode json in either "modern" or old (avro &lt;= 1.4) formats
   * @param schema a schema
   * @param in a String containing a json-serialized avro payload
   * @return a decoder
   * @throws IOException on io errors
   */
  public static Decoder newCompatibleJsonDecoder(Schema schema, String in) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newCompatibleJsonDecoder(schema, in);
  }

  /**
   * {@link Decoder} that performs type-resolution between the reader's and writer's schemas.
   * @param writer writer schema
   * @param reader reader schema
   * @param in a String containing a json-serialized avro payload
   * @return a decoder
   * @throws IOException on io errors
   */
  public static SkipDecoder newCachedResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newCachedResolvingDecoder(writer, reader, in);
  }

  /**
   * {@link Decoder} that fixes a bug in the BinaryDecoder that can cause OutOfMemoryError
   * when deserializing corrupt data or deserializing with the incorrect schema.
   * @param in InputStream
   * @return a decoder
   * @throws IOException on io errors
   */
  public static Decoder newBoundedMemoryDecoder(InputStream in) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newBoundedMemoryDecoder(in);
  }

  /**
   * {@link Decoder} that fixes a bug in the BinaryDecoder that can cause OutOfMemoryError
   * when deserializing corrupt data or deserializing with the incorrect schema.
   * @param data byteArray
   * @return a decoder
   * @throws IOException on io errors
   */
  public static Decoder newBoundedMemoryDecoder(byte[] data) throws IOException {
    assertAvroAvailable();
    return ADAPTER.newBoundedMemoryDecoder(data);
  }

  public static <T> SpecificDatumReader<T> newAliasAwareSpecificDatumReader(Schema writerSchema, Class<T> readerClass) {
    assertAvroAvailable();
    return ADAPTER.newAliasAwareSpecificDatumReader(writerSchema, readerClass);
  }

  // schema parsing, and other Schema-related operations

  /**
   * parses a single avro schema json String (see <a href="https://avro.apache.org/docs/current/spec.html#schemas">spec</a>),
   * which may refer to others.
   * @param schemaJson schema json to parse
   * @param desiredConf desired parse configuration. null for strictest possible. support may vary by runtime avro version :-(
   * @param known other "known" (already parsed) schemas that the given schema json may refer to - or null if none.
   * @return parsing results
   */
  public static SchemaParseResult parse(String schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known) {
    assertAvroAvailable();
    return ADAPTER.parse(schemaJson, desiredConf, known);
  }

  /**
   * concatenates all the string arguments into a single avsc schema and parses that.
   * intended to be called by generated code (because string literals in java source
   * code cannot exceed 64K and some SCHEMA$ fields are over that).
   * NOTE: this method uses loose validation for broad compatibility
   * @param schemaPieces an avro schema in json form. potentially in pieces (because of java restrictions on the size of a string literal)
   * @return a {@link Schema}
   */
  public static Schema parse(String... schemaPieces) {
    if (schemaPieces == null || schemaPieces.length == 0) {
      throw new IllegalArgumentException("must be given at least one argument");
    }
    String wholeSchema;
    if (schemaPieces.length > 1) {
      StringBuilder sb = new StringBuilder();
      for (String piece : schemaPieces) {
        sb.append(piece);
      }
      wholeSchema = sb.toString();
    } else {
      wholeSchema = schemaPieces[0];
    }
    return parse(wholeSchema, SchemaParseConfiguration.LOOSE, null).getMainSchema();
  }

  /**
   * parses a single avro schema json InputStream (see <a href="https://avro.apache.org/docs/current/spec.html#schemas">spec</a>),
   * which may refer to others.
   * @param schemaJson schema json to parse (as an input stream). encoding assumed to be UTF-8.
   * @param desiredConf desired parse configuration. null for strictest possible. support may vary by runtime avro version :-(
   * @param known other "known" (already parsed) schemas that the given schema json may refer to - or null if none.
   * @return parsing results
   */
  public static SchemaParseResult parse(InputStream schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (InputStreamReader reader = new InputStreamReader(schemaJson, StandardCharsets.UTF_8)) {
      char[] buffer = new char[8192];
      int charsRead = reader.read(buffer);
      while (charsRead > -1) {
        sb.append(buffer, 0, charsRead);
        charsRead = reader.read(buffer);
      }
    }
    return parse(sb.toString(), desiredConf, known);
  }

  /**
   * Returns "Parsing Canonical Form" of a schema as defined by the Avro spec for avro 1.7+
   * @param s a schema
   * @return parsing canonical form for the given schema
   */
  public static String toParsingForm(Schema s) {
    assertAvroAvailable();
    return ADAPTER.toParsingForm(s);
  }

  /**
   * Returns the default value for a schema field, as a json string
   * @param field a schema field
   * @return the default value of the field (if such a value exists),
   *         as a json string.
   * @throws org.apache.avro.AvroRuntimeException if the field in question has no default.
   */
  public static String getDefaultValueAsJsonString(Schema.Field field) {
    assertAvroAvailable();
    return ADAPTER.getDefaultValueAsJsonString(field);
  }

  // methods for dealing with specific records (classes generated by avro)

  /**
   * Return true if the {@link IndexedRecord} is a {@link org.apache.avro.specific.SpecificRecord}.
   *
   * This can be a source of confusion in avro 1.7+ because SpecificRecordBase implements GenericRecord
   * so it would be wrong to check for instanceof GenericRecord!
   *
   * @param indexedRecord the record in question
   * @return true if argument is a specific record
   */
  public static boolean isSpecificRecord(IndexedRecord indexedRecord) {
    return indexedRecord instanceof SpecificRecord;
  }

  /**
   * instantiates a possibly-SchemaConstructable class <br>
   * avro defines an interface called SchemaConstructable, which was moved in avro 1.6+
   * (org.apache.avro.specific.SpecificDatumReader.SchemaConstructable in avro &lt;= 1.5
   * but org.apache.avro.specific.SpecificData.SchemaConstructable in avro 1.6+).
   * such classes can only be instantiated by providing a schema as an argument (validation of the
   * schema is the constructor author's responsibility). this method allows for instantiating such
   * classes correctly. if the class in question is not SchemaConstructable the default (no arg)
   * constructor will be invoked
   *
   * @param clazz a class to be instantiated (possibly a SchemaConstructable)
   * @param schema schema to be used if the class is indeed a SchemaConstructable
   * @return an instance of the class
   */
  public static Object newInstance(Class<?> clazz, Schema schema) {
    assertAvroAvailable();
    return ADAPTER.newInstance(clazz, schema);
  }

  /**
   * Returns the STRICTLY LEGAL default value for a schema field, as a specific record class
   * (if the default value is complex enough - say records, enums, fixed fields etc)
   * or as a JDK/Avro class (for simple values like Strings or booleans). <br>
   *
   * this method strictly validates the default value conforms to the schema, which means it
   * may throw for schemas that were parsed with loose validation or under old avro.
   * it will also throw if the field in question has no default value.
   *
   * @param field a schema field
   * @return the default value of the field (if such a value exists),
   *         as a specific record class. may be null.
   * @throws org.apache.avro.AvroRuntimeException if the field in question has no default.
   * @throws org.apache.avro.AvroTypeException if the default value for the field is illegal
   */
  public static Object getSpecificDefaultValue(Schema.Field field) {
    assertAvroAvailable();
    return ADAPTER.getSpecificDefaultValue(field);
  }

  // methods for dealing with generic records

  /**
   * This method is deprecated. Please use {@link AvroCompatibilityHelper#isGenericDataRecord} instead.
   *
   * @param indexedRecord a record
   * @return true if argument is a {@link GenericData.Record}.
   */
  @Deprecated
  public static boolean isGenericRecord(IndexedRecord indexedRecord) {
    return isGenericDataRecord(indexedRecord);
  }

  /**
   * Return true if the {@link IndexedRecord} is a {@link GenericData.Record}.
   *
   * This can be a source of confusion in avro 1.7+ because SpecificRecordBase implements {@link org.apache.avro.generic.GenericRecord}
   * so it would be wrong to check for instanceof GenericRecord!
   *
   * @param indexedRecord a record
   * @return true if argument is a {@link GenericData.Record}.
   */
  public static boolean isGenericDataRecord(IndexedRecord indexedRecord) {
    return !(isSpecificRecord(indexedRecord));
  }

  /**
   * creates a new {@link org.apache.avro.generic.GenericData.EnumSymbol} of the given schema with the given value
   * @param enumSchema enum schema
   * @param enumValue enum value (symbol)
   * @return a new {@link org.apache.avro.generic.GenericData.EnumSymbol}
   */
  public static GenericData.EnumSymbol newEnumSymbol(Schema enumSchema, String enumValue) {
    return newEnumSymbol(enumSchema, enumValue, true);
  }

  /**
   * creates a new {@link org.apache.avro.generic.GenericData.EnumSymbol} of the given schema with the given value
   * @param enumSchema enum schema
   * @param enumValue enum value (symbol)
   * @param assertValueInSymbols true to check that the given symbol is in the given enum schema
   * @return a new {@link org.apache.avro.generic.GenericData.EnumSymbol}
   */
  public static GenericData.EnumSymbol newEnumSymbol(Schema enumSchema, String enumValue, boolean assertValueInSymbols) {
    assertAvroAvailable();
    assertSchemaType(enumSchema, Schema.Type.ENUM);
    if (assertValueInSymbols) {
      boolean match = false;
      List<String> expected = enumSchema.getEnumSymbols();
      for (String legalValue : expected) {
        if (legalValue.equals(enumValue)) {
          match = true;
          break;
        }
      }
      if (!match) {
        String given = enumValue == null ? "a null value" : enumValue;
        throw new IllegalArgumentException(given + " is not a symbol of enum "
                + enumSchema.getFullName() + ". symbols are " + expected);
      }
    } else if (enumValue == null) { //we test for null anyway
      throw new IllegalArgumentException("a null value is not a symbol of enum " + enumSchema.getFullName());
    }
    return ADAPTER.newEnumSymbol(enumSchema, enumValue);
  }

  /**
   * creates a new {@link org.apache.avro.generic.GenericData.Fixed} of the given schema with a value of zeroes
   * @param fixedSchema fixed schema
   * @return a new {@link org.apache.avro.generic.GenericData.Fixed} with a value of all zeroes of the correct size
   */
  public static GenericData.Fixed newFixed(Schema fixedSchema) {
    assertAvroAvailable();
    assertSchemaType(fixedSchema, Schema.Type.FIXED);
    return ADAPTER.newFixedField(fixedSchema);
  }

  /**
   * creates a new {@link org.apache.avro.generic.GenericData.Fixed} of the given schema with the given value
   * @param fixedSchema fixed schema
   * @param contents initial contents of the instance.
   * @return a new {@link org.apache.avro.generic.GenericData.Fixed}
   */
  public static GenericData.Fixed newFixed(Schema fixedSchema, byte[] contents) {
    assertAvroAvailable();
    assertSchemaType(fixedSchema, Schema.Type.FIXED);
    int fixedSize = fixedSchema.getFixedSize();
    if (contents == null || contents.length != fixedSize) {
      String given = (contents == null) ? null : (contents.length + " bytes");
      throw new IllegalArgumentException("contents should be exactly " + fixedSize + " bytes. instead provided " + given);
    }
    return ADAPTER.newFixedField(fixedSchema, contents);
  }

  /**
   * creates a new {@link org.apache.avro.generic.GenericData.Fixed} of the given schema with a value of zeroes
   * @param fixedSchema fixed schema
   * @return a new {@link org.apache.avro.generic.GenericData.Fixed} with a value of all zeroes of the correct size
   * @deprecated use {@link #newFixed(Schema)}
   */
  @Deprecated
  public static GenericData.Fixed newFixedField(Schema fixedSchema) {
    return newFixed(fixedSchema);
  }

  /**
   * creates a new {@link org.apache.avro.generic.GenericData.Fixed} of the given schema with the given value
   * @param fixedSchema fixed schema
   * @param contents initial contents of the instance.
   * @return a new {@link org.apache.avro.generic.GenericData.Fixed}
   * @deprecated use {@link #newFixed(Schema, byte[])}
   */
  @Deprecated
  public static GenericData.Fixed newFixedField(Schema fixedSchema, byte[] contents) {
    return newFixed(fixedSchema, contents);
  }

  public static boolean fieldHasDefault(Schema.Field field) {
    assertAvroAvailable();
    return ADAPTER.fieldHasDefault(field);
  }

  public static boolean defaultValuesEqual(Schema.Field a, Schema.Field b, boolean looseNumerics) {
    assertAvroAvailable();
    return ADAPTER.defaultValuesEqual(a, b, looseNumerics);
  }

  public static Set<String> getFieldAliases(Schema.Field field) {
    assertAvroAvailable();
    return ADAPTER.getFieldAliases(field);
  }

  /**
   * returns the STRICTLY LEGAL default value for a schema field, as a generic record class
   * (if the default value is complex enough - say records, enums, fixed fields etc)
   * or as a JDK/Avro class (for simple values like Strings or booleans). <br>
   *
   * this method strictly validates the default value conforms to the schema, which means it
   * may throw for schemas that were parsed with loose validation or under old avro.
   * it will also throw if the field in question has no default value.
   *
   * @param field a schema field
   * @return the default value of the field (if such a value exists),
   *         as a generic record class. may be null (only if the default value is a legal null)
   * @throws org.apache.avro.AvroRuntimeException if the field in question has no default.
   * @throws org.apache.avro.AvroTypeException if the default value for the field is illegal
   */
  public static Object getGenericDefaultValue(Schema.Field field) {
    assertAvroAvailable();
    return ADAPTER.getGenericDefaultValue(field);
  }

  /**
   * returns the default value for a schema field, as a generic record class
   * (if the default value is complex enough - say records, enums, fixed fields etc)
   * or as a JDK/Avro class (for simple values like Strings or booleans). <br>
   *
   * this variant is more forgiving of foelds with no or illegal default values,
   * and returns null for those cases. it may also return null if thats the actual
   * legal default value for the field. users who case about this distinction should
   * use {@link #getGenericDefaultValue(Schema.Field)} instead
   *
   * @param field a schema field
   * @return the default value of the field (if such a value exists and is legal),
   *         as a generic record class. Returns null if there is no or illegal default value.
   *         A null default also means that field is of type NULL or union field with null
   *         and null being the first member. If null default value is returned,
   *         if needed choose appropriate default value based on schema type.
   */
  public static Object getNullableGenericDefaultValue(Schema.Field field) {
    if (!fieldHasDefault(field)) {
      return null;
    }
    try {
      return getGenericDefaultValue(field);
    } catch (Exception ignored) {
      //(likely) means bad default - we cant catch a "proper" AvroTypeException here
      //because this would cause the helper class to fail to load in case there's no avro
      //on the CP (which is a use case ...)
      return null;
    }
  }

  /**
   * returns a new FieldBuilder, optionally copying the initial values from a given Schema$Field
   * @param other (optional) a field from which to set the initial values for the returned builder
   * @return a new field builder
   */
  public static FieldBuilder newField(Schema.Field other) {
    assertAvroAvailable();
    return ADAPTER.newFieldBuilder(other);
  }

  /**
   * returns a FieldBuilder, containing an existing schema field.
   * @param field a schema field
   * @return a new FieldBuilder
   * @throws org.apache.avro.AvroRuntimeException if the field in question has no default.
   * @deprecated use {@link #newField(Schema.Field)}
   */
  @Deprecated
  public static FieldBuilder cloneSchemaField(Schema.Field field) {
    return newField(field);
  }

  /**
   * clones a given schema, returning a (mutable) builder
   * @param schema schema to clone
   * @return a new {@link SchemaBuilder}
   * @deprecated use {@link #newSchema(Schema)}
   */
  public static SchemaBuilder cloneSchema(Schema schema) {
    return newSchema(schema);
  }

  /**
   * returns a new {@link SchemaBuilder}, optionally setting the initial state to match a given input schema
   * @param schema an optional schema to start by cloning. can be null.
   * @return a new {@link SchemaBuilder}
   */
  public static SchemaBuilder newSchema(Schema schema) {
    assertAvroAvailable();
    return ADAPTER.newSchemaBuilder(schema);
  }

  // code generation

  /**
   * generated java code (specific classes) for a given set of Schemas. this method fails if any failure occurs
   * during code generation
   * @param toCompile set of schema objects
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @param config configuration for controlling aspects of the generated code
   * @return a collection of generated java file descriptors, each with a relative path and proposed contents
   */
  public static Collection<AvroGeneratedSourceCode> compile(
      Collection<Schema> toCompile,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion,
      CodeGenerationConfig config
  ) {
    assertAvroAvailable(); //in this case it still doesnt guarantee the (separate) compiler jar is present for avro 1.5+
    assertCompilerMatchesAvro();
    return ADAPTER.compile(toCompile, minSupportedVersion, maxSupportedVersion, config);
  }

  public static Collection<AvroGeneratedSourceCode> compile(
          Collection<Schema> toCompile,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion
  ) {
    return compile(toCompile, minSupportedVersion, maxSupportedVersion, CodeGenerationConfig.COMPATIBLE_DEFAULTS);
  }

  private AvroCompatibilityHelper() {
    //this is a util class. dont build one yourself
  }

  private static void assertAvroAvailable() {
    if (DETECTED_VERSION == null) {
      throw new IllegalStateException("no version of avro found on the classpath");
    }
  }

  private static void assertNoCompilerDetectionIssues() {
    if (COMPILER_DETECTION_ISSUE != null) {
      throw new IllegalStateException("unable to detect runtime version of avro-compiler jar", COMPILER_DETECTION_ISSUE);
    }
  }

  private static void assertCompilerMatchesAvro() {
    assertNoCompilerDetectionIssues();

    if (DETECTED_COMPILER_VERSION != null && DETECTED_VERSION != DETECTED_COMPILER_VERSION) {
      String msg = "ERROR: version of avro (" + DETECTED_VERSION +") does not match version of avro-compiler ("
          + DETECTED_COMPILER_VERSION + "). sources used to detect avro: " + VersionDetectionUtil.uniqueSourcesForCoreAvro()
          + ". sources used to detect avro-compiler: " + VersionDetectionUtil.uniqueSourcesForAvroCompiler()
          + ". THIS WILL BECOME A FATAL ERROR SOON";
      System.err.println(msg);
      //TODO - throw in the future
    }
  }

  private static void assertSchemaType(Schema schema, Schema.Type wanted) {
    if (schema == null) {
      throw new IllegalArgumentException("must provide a schema. got null");
    }
    Schema.Type given = schema.getType();
    if (!wanted.equals(given)) {
      throw new IllegalArgumentException("must provide a " + wanted
              + " schema. instead got a " + given + ": " + schema);
    }
  }


  private static AvroVersion detectAvroCompilerVersion() throws Exception {
    Class<?> oldCompilerClass = null;
    Class<?> newCompilerClass = null;

    try {
      oldCompilerClass = Class.forName("org.apache.avro.specific.SpecificCompiler");
      VersionDetectionUtil.markUsedForAvroCompiler(oldCompilerClass);
    } catch (Throwable ignored) {
      //empty
    }
    try {
      newCompilerClass = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler");
      VersionDetectionUtil.markUsedForAvroCompiler(newCompilerClass);
    } catch (Throwable ignored) {
      //empty
    }

    if (oldCompilerClass != null && newCompilerClass != null) {
      throw new IllegalStateException("found both old (1.4) and modern avro compiler classes on the classpath");
    }
    if (oldCompilerClass == null && newCompilerClass == null) {
      return null;
    }

    if (oldCompilerClass != null) {
      return AvroVersion.AVRO_1_4;
    }

    //dealing with avro 1.5+ from now on

    // SpecificCompiler.isUnboxedJavaTypeNullable(Schema s) method was added to 1.6.0.
    try {
      newCompilerClass.getMethod("isUnboxedJavaTypeNullable", Schema.class);
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_5;
    }

    // In AVRO-1094, a new property was added to velocityEngine in version 1.7.0.
    // There were no differences in the public interface, so we check for the presence of a property in the private
    // field, `velocityEngine`.
    // To further complicate matters, this field is used in 1.7.0 and removed by 1.10.2. So the presence
    // of the field only indicates that version is one of 1.6, 1.10+.

    // SpecificCompiler.hasBuilder(Schema s) method was added in 1.8.0
    try {
      newCompilerClass.getMethod("hasBuilder", Schema.class);
    } catch (NoSuchMethodException expected) {
      if (checkIfVelocityEngineFileResourceLoaderClassExists(newCompilerClass)) {
        return AvroVersion.AVRO_1_7;
      } else {
        return AvroVersion.AVRO_1_6;
      }
    }

    //SpecificCompiler.isGettersReturnOptional() method was added in 1.9.0.
    try {
      newCompilerClass.getMethod("isGettersReturnOptional");
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_8;
    }

    //SpecificCompiler.isOptionalGettersForNullableFieldsOnly() method was added in 1.10.0.
    try {
      newCompilerClass.getMethod("isOptionalGettersForNullableFieldsOnly");
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_9;
    }

    // SpecificCompiler.getUsedCustomLogicalTypeFactories() method was added in 1.11.0.
    try {
      newCompilerClass.getMethod("getUsedCustomLogicalTypeFactories", Schema.class);
    } catch (NoSuchMethodException expected) {
      return AvroVersion.AVRO_1_10;
    }

    return AvroVersion.AVRO_1_11;
  }

  // We are fishing for the ```file.resource.loader.class``` property in the private field `velocityEngine` of
  // a SpecificCompiler class.
  private static boolean checkIfVelocityEngineFileResourceLoaderClassExists(Class<?> specificCompilerClass) throws Exception {
    // If `velocityEngine` were public, we could simplify this reflection to be
    // ```VelocityEngine velocityEngine = (new SpecificCompiler()).velocityEngine;````
    Field velocityEngineField = specificCompilerClass.getDeclaredField("velocityEngine");
    velocityEngineField.setAccessible(true);
    // SpecificCompiler's default constructor is also private.
    Constructor<?> constructor = specificCompilerClass.getDeclaredConstructor();
    constructor.setAccessible(true);
    constructor.newInstance();
    Object velocityEngine = velocityEngineField.get(constructor.newInstance());

    // Without reflection, this would be ```velocityEngine.getProperty("file.resource.loader.class");```
    Class<?> velocityEngineClass = Class.forName("org.apache.velocity.app.VelocityEngine");
    VersionDetectionUtil.markUsedForAvroCompiler(velocityEngineClass);
    Method velocityEngineGetPropertyMethod = velocityEngineClass.getMethod("getProperty", String.class);
    Object fileResourceLoaderClassProperty =
        velocityEngineGetPropertyMethod.invoke(velocityEngine, "file.resource.loader.class");

    return fileResourceLoaderClassProperty != null;
  }

  /**
   * Get the full schema name for records or type name for primitives. This adds compatibility
   * layer for {@link Schema#getFullName} implementation in avro 1.4, which defaults to throwing exception.
   *
   * @param schema the schema whose full name should be retrieved
   * @return full schema name or primitive type name
   */
  public static String getSchemaFullName(Schema schema) {
    try {
      return schema.getFullName();
    } catch (RuntimeException e) {
      return schema.getName();
    }
  }

  /**
   * Create a new schema field with provided arguments. This adds compatibility layer for {@link Schema.Field} before
   * avro 1.9, which takes JsonNode as default value in constructor instead of Object.
   * @param name the name of the field
   * @param schema the schema of the field
   * @param doc the doc of the field
   * @param defaultValue the default value of the field. For avro 1.4-1.8, user should pass in a jackson1 JsonNode.
   *                     For avro 1.9 and newer, user should pass in the default value object directly.
   * @param order the order of the field.
   * @return fully constructed Field instance
   */
  public static Schema.Field createSchemaField(String name, Schema schema, String doc, Object defaultValue, Schema.Field.Order order) {
    return newField(null)
            .setName(name)
            .setSchema(schema)
            .setDoc(doc)
            .setDefault(defaultValue)
            .setOrder(order)
            .build();
  }

  public static Schema.Field createSchemaField(String name, Schema schema, String doc, Object defaultValue) {
    return createSchemaField(name, schema, doc, defaultValue, Schema.Field.Order.ASCENDING);
  }

  /**
   * returns the value of the specified field prop as a json literal.
   * returns null if the field has no such property.
   * note that string values are returned quoted (as a proper json string literal)
   * @param field the field who's property value we wish to get
   * @param propName the name of the property
   * @return field property value as json literal, or null if no such property
   */
  public static String getFieldPropAsJsonString(Schema.Field field, String propName) {
    return getFieldPropAsJsonString(field, propName, true, false);
  }

  /**
   * returns the value of the specified field prop as a json literal.
   * returns null if the field has no such property.
   * optionally returns string literals as "naked" strings. also optionally unescapes any nested json
   * inside such nested strings
   * @param field the field who's property value we wish to get
   * @param propName the name of the property
   * @param quoteStringValues true to return string literals quoted. false to strip such quotes. false matches avro behaviour
   * @param unescapeInnerJson true to unescape inner json inside string literals. true matches avro behaviour
   * @return field property value as json literal, or null if no such property
   */
  public static String getFieldPropAsJsonString(Schema.Field field, String propName, boolean quoteStringValues, boolean unescapeInnerJson) {
    assertAvroAvailable();
    String value = ADAPTER.getFieldPropAsJsonString(field, propName);
    return StringUtils.unquoteAndUnescapeStringProp(value, quoteStringValues, unescapeInnerJson);
  }

  /**
   * Sets a field's property to the given value. None of the arguments can be null. It is illegal to add a property if
   * another with the same name but different value already exists in this field.
   *
   * If strict is true, the value MUST be a single, proper JSON literal:
   * <ul>
   *   <li>
   *     In Avro &lt;= 1.7.2, which only supports string-valued props, it must be a proper string literal (with
   *     surrounding double-quotes and escaping within).
   *   </li>
   *   <li>
   *     In Avro &gt;= 1.7.3, it will be parsed (deserialized) into an object.
   *   </li>
   * </ul>
   *
   * If strict is false: If the value is a single, proper JSON literal, it's treated the same way as above; If it's not
   * a valid literal, it's treated as an unquoted, unescaped string value.
   *
   * @param field the field whose property is being set.
   * @param propName the name of the property.
   * @param valueAsJsonLiteral the value of the property as a JSON literal.
   * @param strict see description for details.
   */
  public static void setFieldPropFromJsonString(Schema.Field field, String propName, String valueAsJsonLiteral, boolean strict) {
    assertAvroAvailable();
    ADAPTER.setFieldPropFromJsonString(field, propName, valueAsJsonLiteral, strict);
  }

  public static boolean sameJsonProperties(Schema.Field a, Schema.Field b, boolean compareStringProps, boolean compareNonStringProps) {
    assertAvroAvailable();
    return ADAPTER.sameJsonProperties(a, b, compareStringProps, compareNonStringProps);
  }

  /**
   * returns the value of the specified schema prop as a json literal.
   * returns null if the schema has no such property.
   * note that string values are returned quoted (as a proper json string literal)
   * @param schema the schema who's property value we wish to get
   * @param propName the name of the property
   * @return schema property value as json literal, or null if no such property
   */
  public static String getSchemaPropAsJsonString(Schema schema, String propName) {
    return getSchemaPropAsJsonString(schema, propName, true, false);
  }

  /**
   * returns the value of the specified schema prop as a json literal.
   * returns null if the schema has no such property.
   * optionally returns string literals as "naked" strings. also optionally unescapes any nested json
   * inside such nested strings
   * @param schema the schema who's property value we wish to get
   * @param propName the name of the property
   * @param quoteStringValues true to return string literals quoted. false to strip such quotes. false matches avro behaviour
   * @param unescapeInnerJson true to unescape inner json inside string literals. true matches avro behaviour
   * @return schema property value as json literal, or null if no such property
   */
  public static String getSchemaPropAsJsonString(Schema schema, String propName, boolean quoteStringValues, boolean unescapeInnerJson) {
    assertAvroAvailable();
    String value = ADAPTER.getSchemaPropAsJsonString(schema, propName);
    return StringUtils.unquoteAndUnescapeStringProp(value, quoteStringValues, unescapeInnerJson);
  }

  /**
   * Sets a schema's property to the given value. None of the arguments can be null. It is illegal to add a property if
   * another with the same name but different value already exists in this schema.
   *
   * If strict is true, the value MUST be a single, proper JSON literal:
   * <ul>
   *   <li>
   *     In Avro &lt;= 1.7.2, which only supports string-valued props, it must be a proper string literal (with
   *     surrounding double-quotes and escaping within).
   *   </li>
   *   <li>
   *     In Avro &gt;= 1.7.3, it will be parsed (deserialized) into an object.
   *   </li>
   * </ul>
   *
   * If strict is false: If the value is a single, proper JSON literal, it's treated the same way as above; If it's not
   * a valid literal, it's treated as an unquoted, unescaped string value.
   *
   * @param schema the field whose property is being set.
   * @param propName the name of the property.
   * @param valueAsJsonLiteral the value of the property as a JSON literal.
   * @param strict see description for details.
   */
  public static void setSchemaPropFromJsonString(Schema schema, String propName, String valueAsJsonLiteral, boolean strict) {
    assertAvroAvailable();
    ADAPTER.setSchemaPropFromJsonString(schema, propName, valueAsJsonLiteral, strict);
  }

  public static boolean sameJsonProperties(Schema a, Schema b, boolean compareStringProps, boolean compareNonStringProps) {
    assertAvroAvailable();
    return ADAPTER.sameJsonProperties(a, b, compareStringProps, compareNonStringProps);
  }

  public static List<String> getAllPropNames(Schema schema) {
    assertAvroAvailable();
    return ADAPTER.getAllPropNames(schema);
  }

  public static List<String> getAllPropNames(Schema.Field field) {
    assertAvroAvailable();
    return ADAPTER.getAllPropNames(field);
  }

  /**
   * returns the enum default value.
   * returns null if schema has no such property.
   *
   * @param schema the schema who's property value we wish to get
   * @return enum default value, or null if no such property
   */
  public static String getEnumDefault(Schema schema) {
    assertAvroAvailable();
    try {
      return ADAPTER.getEnumDefault(schema);
    } catch (RuntimeException e) {
      return null;
    }
  }

  public static Schema newEnumSchema(String name, String doc, String namespace, List<String> values,
      String enumDefault) {
    assertAvroAvailable();
    return ADAPTER.newEnumSchema(name, doc, namespace, values, enumDefault);
  }

  /**
   * given a schema, returns a (exploded, fully-inlined, self-contained, however you want to call it)
   * avsc representation of the schema.
   * this is logically the same as {@link Schema#toString(boolean)} except not full of horrible bugs.
   * specifically, under all versions of avro, we support:
   * <ul>
   *     <li>output free of avro-702, even under avro 1.4</li>
   *     <li>escaped characters in docs and default values remain properly escaped (avro-886)</li>
   * </ul>
   * (unless of course you choose to delegate to vanilla avro, at which point youre at the mercy of
   * the runtime version thereof)
   * @param schema a schema to serialize to avsc
   * @param config configuration for avsc generation. see {@link AvscGenerationConfig} for available knobs
   * @return avsc
   */
  public static String toAvsc(Schema schema, AvscGenerationConfig config) {
    assertAvroAvailable();
    if (config == null) {
      throw new IllegalArgumentException("config must be provided");
    }
    return ADAPTER.toAvsc(schema, config);
  }

  /***
   * Check if Fields in schema are reordered
   * @param schema
   * @return true / false
   */
  public static boolean areFieldsReordered(Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      if(i != fields.get(i).pos()) {
        return true;
      }
    }
    return false;
  }

  /**
   * given a schema, returns a (exploded, fully-inlined, self-container, however you want to call it)
   * avsc representation of the schema.
   * this is logically the same as {@link Schema#toString(boolean)} except not full of horrible bugs.
   * specifically, under all versions of avro:
   * <ul>
   *     <li>the output is free of avro-702, even under avro 1.4</li>
   *     <li>escaped characters in docs and default values remain properly escaped under avro &lt; 1.6</li>
   * </ul>
   * @param schema a schema to serialize to avsc
   * @param pretty true to return a pretty-printed schema, false for single-line
   * @return avsc
   * @deprecated please use {@link #toAvsc(Schema, AvscGenerationConfig)} directly
   */
  @Deprecated
  public static String toAvsc(Schema schema, boolean pretty) {
    assertAvroAvailable();
    return ADAPTER.toAvsc(schema, pretty ? AvscGenerationConfig.CORRECT_PRETTY : AvscGenerationConfig.CORRECT_ONELINE);
  }

  /**
   * given a schema returns an exploded avsc representation of the schema using the old, horrible
   * avro 1.4 avro-702-susceptible logic.
   * this might be required for bug-to-bug-compatibility with legacy code, but should otherwise be avoided
   * @param schema a schema to serialize to (possibly bad) avsc
   * @param pretty true to return a pretty-printed schema, false for single-line
   * @return possibly bad avsc
   * @deprecated please use {@link #toAvsc(Schema, AvscGenerationConfig)} directly
   */
  @Deprecated
  public static String toBadAvsc(Schema schema, boolean pretty) {
    assertAvroAvailable();
    return ADAPTER.toAvsc(schema,
            pretty ?
                    AvscGenerationConfig.LEGACY_PRETTY
                  : AvscGenerationConfig.LEGACY_ONELINE
    );
  }
}
