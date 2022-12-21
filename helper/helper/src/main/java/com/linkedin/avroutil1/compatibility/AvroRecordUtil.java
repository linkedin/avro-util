/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;


public class AvroRecordUtil {
  private static final List<StringRepresentation> STRING_ONLY = Collections.singletonList(StringRepresentation.String);
  private static final List<StringRepresentation> UTF8_PREFERRED = Collections.unmodifiableList(Arrays.asList(
      StringRepresentation.Utf8, StringRepresentation.String
  ));

  /**
   * field names that avro will avoid and instead append a "$" to.
   * see {@link org.apache.avro.specific.SpecificCompiler}.RESERVED_WORDS and mangle()
   */
  public final static Set<String> AVRO_RESERVED_FIELD_NAMES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "abstract", "assert", "boolean", "break", "byte", "case", "catch",
      "char", "class", "const", "continue", "default", "do", "double",
      "else", "enum", "extends", "false", "final", "finally", "float",
      "for", "goto", "if", "implements", "import", "instanceof", "int",
      "interface", "long", "native", "new", "null", "package", "private",
      "protected", "public", "return", "schema", "short", "static", "strictfp",
      "super", "switch", "synchronized", "this", "throw", "throws",
      "transient", "true", "try", "void", "volatile", "while"
  )));

  private AvroRecordUtil() {
    //utility class
  }

  /**
   * sets all fields who's value is not allowed according to their schema to their default values (as specified in their schema)
   * optionally throws if field is not set to a valid value yet schema has no default for it
   * @param record a record to recursively supplement defaults in
   * @param throwIfMissingValuesLeft true to throw if field value is invalid yet no default exists
   * @param <T> exact type of record
   * @return the input record
   */
  public static <T extends IndexedRecord> T supplementDefaults(T record, boolean throwIfMissingValuesLeft) {
    if (record == null) {
      throw new IllegalArgumentException("record argument required");
    }
    boolean isSpecific = AvroCompatibilityHelper.isSpecificRecord(record);
    //noinspection unchecked
    return (T) supplementDefaults(record, throwIfMissingValuesLeft, isSpecific);
  }

  /**
   * sets all fields who's value is not allowed according to their schema to their default values (as specified in their schema)
   * optionally throws if field is not set to a valid value yes schema has no default for it
   * @param record a record to DFS
   * @param throwIfMissingValuesLeft true to throw if field value is invalid yet no default exists
   * @param useSpecifics true to populate specific default values, false to populate with generics
   * @return the input record
   */
  private static IndexedRecord supplementDefaults(IndexedRecord record, boolean throwIfMissingValuesLeft, boolean useSpecifics) {
    Schema schema = record.getSchema();
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      Object fieldValue = record.get(field.pos());
      Schema fieldSchema = field.schema();

      boolean populate = true;
      if (AvroSchemaUtil.isValidValueForSchema(fieldValue, fieldSchema)) {
        //this field is fine
        populate = false;
      } else if (!AvroCompatibilityHelper.fieldHasDefault(field)) {
        //no default, yet no value. complain?
        if (throwIfMissingValuesLeft) {
          throw new IllegalArgumentException(schema.getName() + "."  + field.name()
              + " has no default value, yet no value is set on the input record");
        }
        populate = false;
      }

      if (populate) {
        if (useSpecifics) {
          fieldValue = AvroCompatibilityHelper.getSpecificDefaultValue(field);
        } else {
          fieldValue = AvroCompatibilityHelper.getGenericDefaultValue(field);
        }
        record.put(field.pos(), fieldValue);
      }

      if (fieldValue instanceof IndexedRecord) {
        supplementDefaults((IndexedRecord) fieldValue, throwIfMissingValuesLeft, useSpecifics);
      }
    }
    return record;
  }

  /**
   * converts a given {@link GenericData.Record} into an instance of the "equivalent"
   * {@link SpecificRecord} (SR) class. Can optionally reuse a given SR instance as output,
   * otherwise SR class will be looked up on the current thread's classpath by fullname
   * (or optionally by aliases) <br>
   * <b>WARNING:</b> this method is a crutch. If at all possible, configure your avro decoding operation
   * to generate the desired output type - either generics or specifics - directly. it will be FAR cheaper
   * than using this method of conversion.
   * @param input generic record to convert from. required
   * @param outputReuse specific record to convert into. optional. if provided must be of the correct
   *                    fullname (matching input or any of input's aliases, depending on config)
   * @param config configuration for the operation
   * @param <T> type of the SR class
   * @return a SR converted from the input
   */
  public static <T extends SpecificRecord> T genericRecordToSpecificRecord(
          GenericRecord input,
          T outputReuse,
          RecordConversionConfig config
  ) {
    if (input == null) {
      throw new IllegalArgumentException("input required");
    }
    if (config == null) {
      throw new IllegalArgumentException("config required");
    }

    RecordConversionContext context = new RecordConversionContext(config);
    context.setUseSpecifics(true);

    Schema inputSchema = input.getSchema();
    Schema outputSchema;
    ClassLoader cl;
    T outputRecord;
    if (outputReuse == null) {
      //use context loader
      Class<T> srClass;
      cl = Thread.currentThread().getContextClassLoader();
      context.setClassLoader(cl);
      //look up SR class by fullname and possibly aliases
      //noinspection unchecked
      srClass = (Class<T>) context.lookup(inputSchema);
      if (srClass == null) {
        throw new IllegalStateException("unable to find/load class " + inputSchema.getFullName());
      }

      outputSchema = AvroSchemaUtil.getDeclaredSchema(srClass);
      //noinspection unchecked
      outputRecord = (T) AvroCompatibilityHelper.newInstance(srClass, outputSchema);
    } else {
      //use same loader that loaded output
      cl = outputReuse.getClass().getClassLoader();
      context.setClassLoader(cl);
      outputSchema = outputReuse.getSchema();
      //TODO - validate output schema vs input schema
      outputRecord = outputReuse;
    }

    deepConvertRecord(input, outputRecord, context);

    return outputRecord;
  }

  /**
   * converts a given {@link SpecificRecord} (SR) into an instance of the "equivalent"
   * {@link GenericData.Record}. Can optionally reuse a given GR instance as output.
   * otherwise a new GR will be created using the schema from the input SR<br>
   * <b>WARNING:</b> this method is a crutch. If at all possible, configure your avro decoding operation
   * to generate the desired output type - either generics or specifics - directly. it will be FAR cheaper
   * than using this method of conversion.
   * @param input specific record to convert from. required
   * @param outputReuse generic record to convert into. optional. if provided must be of the correct
   *                    fullname (matching input or any of input's aliases, depending on config)
   * @param config configuration for the operation
   * @return a SR converted from the input
   */
  public static GenericRecord specificRecordToGenericRecord(
      SpecificRecord input,
      GenericRecord outputReuse,
      RecordConversionConfig config
  ) {
    if (input == null) {
      throw new IllegalArgumentException("input required");
    }
    if (config == null) {
      throw new IllegalArgumentException("config required");
    }

    RecordConversionContext context = new RecordConversionContext(config);
    context.setUseSpecifics(false);

    Schema inputSchema = input.getSchema();
    Schema outputSchema;
    ClassLoader cl;
    GenericRecord outputRecord;
    if (outputReuse == null) {
      //use context loader
      cl = Thread.currentThread().getContextClassLoader();
      context.setClassLoader(cl);

      outputSchema = inputSchema;
      outputRecord = new GenericData.Record(outputSchema);
    } else {
      //use same loader that loaded output
      cl = outputReuse.getClass().getClassLoader();
      context.setClassLoader(cl);
      outputSchema = outputReuse.getSchema();
      //TODO - validate output schema vs input schema
      outputRecord = outputReuse;
    }

    deepConvertRecord(input, outputRecord, context);

    return outputRecord;
  }

  public static <T extends IndexedRecord> T setStringField(T record, String fieldName, CharSequence value) {
    Schema.Field field = resolveField(record, fieldName);
    if (!(record instanceof SpecificRecord)) {
      //generic record
      //TODO - make this honor schema logical types or runtime avro version?
      record.put(field.pos(), value);
      return record;
    }
    String expectedFieldName = AVRO_RESERVED_FIELD_NAMES.contains(fieldName) ? fieldName + "$" : fieldName;
    Class<? extends IndexedRecord> recordClass = record.getClass();
    //look for public field 1st
    Field[] classFields = recordClass.getFields();
    for (Field classField : classFields) {
      if (expectedFieldName.equals(classField.getName())) {
        Class<?> classFieldType = classField.getType();
        StringRepresentation classFieldStringRep = StringRepresentation.forClass(classFieldType);
        CharSequence convertedValue = toString(value, classFieldStringRep);
        try {
          classField.set(record, convertedValue);
        } catch (Exception e) {
          throw new IllegalStateException("unable to set field " + recordClass.getName() + "." + classField.getName()
              + " to " + classFieldStringRep + " " + convertedValue, e);
        }
        return record;
      }
    }

    //look for setter
    Method[] classMethods = recordClass.getMethods();
    String expectedMethodName = "set" + expectedFieldName.substring(0, 1).toUpperCase(Locale.ROOT) + expectedFieldName.substring(1);
    for (Method method : classMethods) {
      //needs to be called setSomething()
      if (!expectedMethodName.equals(method.getName())) {
        continue;
      }
      //needs to be void
      Class<?> returnType = method.getReturnType();
      if (!Void.TYPE.equals(returnType)) {
        continue;
      }
      //needs to have a single arg
      Class<?>[] args = method.getParameterTypes();
      if (args == null || args.length != 1) {
        continue;
      }
      Class<?> setterArgType = args[0];
      StringRepresentation setterArgStringRep = StringRepresentation.forClass(setterArgType);
      CharSequence convertedValue = toString(value, setterArgStringRep);
      try {
        method.invoke(record, convertedValue);
      } catch (Exception e) {
        throw new IllegalStateException("unable to call setter " + method
            + " to " + setterArgStringRep + " " + convertedValue, e);
      }
      return record;
    }
    throw new IllegalStateException("unable to find either public field of setter method for " + recordClass.getName() + "." + fieldName);
  }

  public static <T extends IndexedRecord> T setField(T record, String fieldName, Object value) {
    Schema.Field field = resolveField(record, fieldName);
    Schema fieldSchema = field.schema();
    Schema destinationSchema = fieldSchema; //unless union (see below)
    if (fieldSchema.getType() == Schema.Type.UNION) {
      //will throw if no matching branch found
      //TODO - catch and provide details of record.field ?
      destinationSchema = AvroSchemaUtil.resolveUnionBranchOf(value, fieldSchema);
    }
    if (!(record instanceof SpecificRecord)) {
      //generic record
      //TODO - make this honor schema logical types or runtime avro version?
      record.put(field.pos(), value);
      return record;
    }
    //specific record class below this point
    Class<? extends IndexedRecord> recordClass = record.getClass();
    //locate the destination - field or method
    Field publicField = findPublicFieldFor(recordClass, fieldName);
    Method setter = findSetterFor(recordClass, fieldName);;
    FieldAccessor accessor = new FieldAccessor(
        ((SpecificRecord) record).getClass(),
        record.getSchema(),
        field,
        publicField,
        setter
    );
    accessor.setValue((SpecificRecord) record, value);
    return record;
  }

  private static Field findPublicFieldFor(Class<? extends IndexedRecord> recordClass, String schemaFieldName) {
    String expectedFieldName = AVRO_RESERVED_FIELD_NAMES.contains(schemaFieldName) ? schemaFieldName + "$" : schemaFieldName;
    //getting all public fields might be faster than getting the desired field by name
    //because ot avoids the exception that would be thrown if its not found
    //TODO - benchmark to prove the above statement
    Field[] publicFields = recordClass.getFields();
    for (Field publicField : publicFields) {
      if (expectedFieldName.equals(publicField.getName())) {
        return publicField;
      }
    }
    return null;
  }

  private static Method findSetterFor(Class<? extends IndexedRecord> recordClass, String schemaFieldName) {
    //bob --> setBob
    String expectedSetterName = "set" + schemaFieldName.substring(0, 1).toUpperCase(Locale.ROOT) + schemaFieldName.substring(1);
    if (AVRO_RESERVED_FIELD_NAMES.contains(schemaFieldName)) {
      //setClass --> setClass$
      expectedSetterName += "$";
    }
    String expectedFieldName =
        AVRO_RESERVED_FIELD_NAMES.contains(schemaFieldName) ? schemaFieldName + "$" : schemaFieldName;
    //going over all public methods might be faster compared to catching
    //an exception trying to get a specific one
    //TODO - benchmark above
    Method[] publicMethods = recordClass.getMethods();
    for (Method method : publicMethods) {
      //needs to be called setSomething()
      if (!expectedSetterName.equals(method.getName())) {
        continue;
      }
      //needs to be void
      Class<?> returnType = method.getReturnType();
      if (!Void.TYPE.equals(returnType)) {
        continue;
      }
      //needs to have a single arg
      Class<?>[] args = method.getParameterTypes();
      if (args == null || args.length != 1) {
        continue;
      }
      return method;
    }
    //no setter
    return null;
  }

  private static Schema.Field resolveField(IndexedRecord record, String fieldName) {
    if (record == null) {
      throw new IllegalArgumentException("record argument required");
    }
    Schema schema = record.getSchema();
    Schema.Field field = schema.getField(fieldName);
    if (field == null) {
      throw new IllegalArgumentException("schema " + schema.getFullName() + " has no such field " + fieldName);
    }
    return field;
  }

  private static void deepConvertRecord(IndexedRecord input, IndexedRecord output, RecordConversionContext context) {
    RecordConversionConfig config = context.getConfig();
    Schema inputSchema = input.getSchema();
    Schema outputSchema = output.getSchema();
    for (Schema.Field outField : outputSchema.getFields()) {
      //look up field on input by name then (optionally) aliases
      Schema.Field inField = findMatchingField(outField, inputSchema, config);
      //grab and convert inField value if found, use outField default if not
      Object outputValue;
      if (inField == null) {
        outputValue = context.isUseSpecifics() ? AvroCompatibilityHelper.getSpecificDefaultValue(outField)
                : AvroCompatibilityHelper.getGenericDefaultValue(outField);
      } else {
        Object inputValue = input.get(inField.pos());
        //figure out what type (in avro) this value is, which is only tricky for unions
        Schema inFieldSchema = inField.schema();
        Schema inValueSchema;
        Schema.Type inFieldSchemaType = inFieldSchema.getType();
        if (inFieldSchemaType == Schema.Type.UNION) {
          boolean inputSpecific = AvroCompatibilityHelper.isSpecificRecord(input);
          int unionBranch;
          if (inputSpecific) {
            unionBranch = SpecificData.get().resolveUnion(inFieldSchema, inputValue);
          } else {
            unionBranch = GenericData.get().resolveUnion(inFieldSchema, inputValue);
          }
          inValueSchema = inFieldSchema.getTypes().get(unionBranch);
        } else {
          inValueSchema = inFieldSchema;
        }

        //figure out the output schema that matches the input value (following the same logic
        //as regular avro decoding)
        Schema outFieldSchema = outField.schema();
        SchemaResolutionResult readerSchemaResolution = AvroSchemaUtil.resolveReaderVsWriter(
                inValueSchema,
                outFieldSchema,
                config.isUseAliasesOnNamedTypes()
                , true
        );
        if (readerSchemaResolution == null) {
          throw new IllegalArgumentException("value for field " + inField.name() + " (" + inValueSchema + " value "
                  + inputValue + ") cannot me resolved to destination schema " + outFieldSchema);
        }
        Schema readerSchema = readerSchemaResolution.getReaderMatch();

        //if reader (destination) field is a string, determine what string representation to use
        StringRepresentation stringRepresentation = context.getConfig().getPreferredStringRepresentation();
        if (readerSchema.getType() == Schema.Type.STRING) {
          if (context.isUseSpecifics()) {
            List<StringRepresentation> fieldPrefs = stringRepForSpecificField((SpecificRecord) output, outField);
            if (fieldPrefs != null && !fieldPrefs.isEmpty()) {
              //are we able to determine what string representation the generated class "prefers" ?
              //TODO - complain if we cant determine string type used by generated code
              if (config.isUseStringRepresentationHints()) {
                stringRepresentation = fieldPrefs.get(0);
              } else {
                if (!fieldPrefs.contains(stringRepresentation)) {
                  //only use field prefs if its physically impossible to use the config pref
                  stringRepresentation = fieldPrefs.get(0);
                }
              }
            }
          } else {
            StringRepresentation fieldPref = stringRepForGenericField(outputSchema, outField);
            if (stringRepresentation != null && config.isUseStringRepresentationHints()) {
              stringRepresentation = fieldPref;
            }
          }
        }

        outputValue = deepConvert(inputValue, inValueSchema, readerSchema, context, stringRepresentation);

      }

      output.put(outField.pos(), outputValue);
    }
  }

  /**
   * find the corresponding "source" field from an input ("writer") record schema
   * that matches a given "output" field from a destination ("reader") record schema
   * @param outField
   * @param inputSchema
   * @param config
   * @return
   */
  private static Schema.Field findMatchingField(Schema.Field outField, Schema inputSchema, RecordConversionConfig config) {
    String outFieldName = outField.name();
    //look up field on input by name then (optionally) aliases
    Schema.Field inField = inputSchema.getField(outFieldName);
    if (inField == null && config.isUseAliasesOnFields()) {
      //~same as avro applying reader schema aliases to writer schema on decoding
      Set<String> fieldAliases = AvroCompatibilityHelper.getFieldAliases(outField); //never null
      for (String fieldAlias : fieldAliases) {
        Schema.Field matchByAlias = inputSchema.getField(fieldAlias);
        if (matchByAlias == null) {
          continue;
        }
        if (inField != null) {
          //TODO - consider better matching by type as well? see what avro decoding does
          throw new IllegalStateException("output field " + outFieldName
                  + " has multiple input fields matching by aliases: " + inField.name() + " and " + fieldAlias);
        }
        inField = matchByAlias;
      }
    }
    return inField;
  }

  protected static Object deepConvert(
          Object inputValue,
          Schema inputSchema,
          Schema outputSchema,
          RecordConversionContext context,
          StringRepresentation stringRepresentation
  ) {
    Schema.Type inputType = inputSchema.getType();
    Schema.Type outputType = outputSchema.getType();

    boolean inputIsUnion;
    boolean outputIsUnion;
    Schema inputValueActualSchema;
    Schema outputValueActualSchema;

    switch (outputType) {

      //primitives

      case NULL:
        if (inputValue != null) {
          throw new IllegalArgumentException("only legal input value for type NULL is null, not " + inputValue);
        }
        return null;
      case BOOLEAN:
        //noinspection RedundantCast - cast serves as input validation
        return (Boolean) inputValue;
      case INT:
        //noinspection RedundantCast - cast serves as input validation
        return (Integer) inputValue;
      case LONG:
        switch (inputType) {
          case INT:
            return ((Integer) inputValue).longValue();
          case LONG:
            //noinspection RedundantCast - cast serves as input validation
            return (Long) inputValue;
        }
        break;
      case FLOAT:
        switch (inputType) {
          case INT:
            return ((Integer) inputValue).floatValue();
          case LONG:
            return ((Long) inputValue).floatValue();
          case FLOAT:
            //noinspection RedundantCast - cast serves as input validation
            return (Float) inputValue;
        }
        break;
      case DOUBLE:
        switch (inputType) {
          case INT:
            return ((Integer) inputValue).doubleValue();
          case LONG:
            return ((Long) inputValue).doubleValue();
          case FLOAT:
            return ((Float) inputValue).doubleValue();
          case DOUBLE:
            //noinspection RedundantCast - cast serves as input validation
            return (Double) inputValue;
        }
        break;
      case BYTES:
        switch (inputType) {
          case BYTES:
            //noinspection RedundantCast - cast serves as input validation
            return (ByteBuffer) inputValue;
          case STRING:
            return ByteBuffer.wrap(((String) inputValue).getBytes(StandardCharsets.UTF_8));
        }
        break;
      case STRING:
        switch (inputType) {
          case BYTES:
            ByteBuffer buf = (ByteBuffer) inputValue;
            if (buf.position() != 0) {
              buf.flip();
            }
            byte[] bytes = new byte[buf.limit()];
            buf.get(bytes);
            return toString(bytes, stringRepresentation);
          case STRING:
            return toString((CharSequence) inputValue, stringRepresentation);
        }
        break;

      //named types

      case FIXED:
        GenericFixed fixedInput = (GenericFixed) inputValue; //works on generics and specifics
        byte[] bytes = fixedInput.bytes();
        if (context.isUseSpecifics()) {
          @SuppressWarnings("unchecked")
          Class<? extends SpecificFixed> fixedClass = (Class<? extends SpecificFixed>) context.lookup(outputSchema);
          SpecificFixed specific = (SpecificFixed) AvroCompatibilityHelper.newInstance(fixedClass, outputSchema);
          specific.bytes(bytes);
          return specific;
        } else {
          return AvroCompatibilityHelper.newFixed(outputSchema, bytes);
        }
      case ENUM:
        String inputSymbolStr;
        if (inputValue instanceof GenericEnumSymbol) {
          inputSymbolStr = inputValue.toString();
        } else if (inputValue instanceof Enum) {
          inputSymbolStr = ((Enum<?>) inputValue).name();
        } else {
          throw new IllegalArgumentException("input " + inputValue + " (a " + inputValue.getClass().getName() + ") not any kind of enum?");
        }
        String outputSymbolStr = inputSymbolStr;
        if (!outputSchema.hasEnumSymbol(inputSymbolStr)) {
          outputSymbolStr = null;
          if (context.getConfig().isUseEnumDefaults()) {
            outputSymbolStr = AvroCompatibilityHelper.getEnumDefault(outputSchema);
          }
        }
        if (outputSymbolStr == null) {
          throw new IllegalArgumentException("cant map input enum symbol " + inputSymbolStr + " to output " + outputSchema.getFullName());
        }
        if (context.isUseSpecifics()) {
          @SuppressWarnings("unchecked")
          Class<? extends Enum<?>> enumClass = (Class<? extends Enum<?>>) context.lookup(outputSchema);
          return getSpecificEnumSymbol(enumClass, outputSymbolStr);
        } else {
          return AvroCompatibilityHelper.newEnumSymbol(outputSchema, outputSymbolStr);
        }
      case RECORD:
        IndexedRecord inputRecord = (IndexedRecord) inputValue;
        IndexedRecord outputRecord;
        if (context.isUseSpecifics()) {
          Class<?> recordClass = context.lookup(outputSchema);
          outputRecord = (IndexedRecord) AvroCompatibilityHelper.newInstance(recordClass, outputSchema);
        } else {
          outputRecord = new GenericData.Record(outputSchema);
        }
        deepConvertRecord(inputRecord, outputRecord, context);
        return outputRecord;

      //collection schemas

      case ARRAY:
        List<?> inputList = (List<?>) inputValue;
        List<Object> outputList;
        if (context.isUseSpecifics()) {
          outputList = new ArrayList<>(inputList.size());
        } else {
          outputList = new GenericData.Array<>(inputList.size(), outputSchema);
        }
        //TODO - add support for collections of unions
        Schema inputElementDeclaredSchema = inputSchema.getElementType();
        inputIsUnion = inputElementDeclaredSchema.getType() == Schema.Type.UNION;
        Schema outputElementDeclaredSchema = outputSchema.getElementType();
        outputIsUnion = outputElementDeclaredSchema.getType() == Schema.Type.UNION;
        for (Object inputElement : inputList) {

          inputValueActualSchema = inputElementDeclaredSchema;
          if (inputIsUnion) {
            inputValueActualSchema = AvroSchemaUtil.resolveUnionBranchOf(inputElement, inputElementDeclaredSchema);
          }
          outputValueActualSchema = outputElementDeclaredSchema;
          if (outputIsUnion) {
            SchemaResolutionResult resolution = AvroSchemaUtil.resolveReaderVsWriter(inputValueActualSchema, outputElementDeclaredSchema, true, true);
            outputValueActualSchema = resolution.getReaderMatch();
            if (outputValueActualSchema == null) {
              throw new UnsupportedOperationException("dont know how to resolve a " + inputValueActualSchema.getType() + " to " + outputElementDeclaredSchema);
            }
          }

          Object outputElement = deepConvert(
              inputElement,
              inputValueActualSchema,
              outputValueActualSchema,
              context,
              stringRepresentation
          );
          outputList.add(outputElement);
        }
        return outputList;

      case MAP:
        @SuppressWarnings("unchecked") //cast serves as input validation
        Map<? extends CharSequence, ?> inputMap = (Map<String, ?>) inputValue;
        Map<CharSequence, Object> outputMap = new HashMap<>(inputMap.size()); //for both generic and specific output
        //TODO - add support for collections of unions
        Schema inputValueDeclaredSchema = inputSchema.getValueType();
        inputIsUnion = inputValueDeclaredSchema.getType() == Schema.Type.UNION;
        Schema outputValueDeclaredSchema = outputSchema.getValueType();
        outputIsUnion = outputValueDeclaredSchema.getType() == Schema.Type.UNION;
        for (Map.Entry<? extends CharSequence, ?> entry : inputMap.entrySet()) {
          CharSequence key = entry.getKey();
          Object inputElement = entry.getValue();

          inputValueActualSchema = inputValueDeclaredSchema;
          if (inputIsUnion) {
            inputValueActualSchema = AvroSchemaUtil.resolveUnionBranchOf(inputElement, inputValueDeclaredSchema);
          }
          outputValueActualSchema = outputValueDeclaredSchema;
          if (outputIsUnion) {
            SchemaResolutionResult resolution = AvroSchemaUtil.resolveReaderVsWriter(inputValueActualSchema, outputValueDeclaredSchema, true, true);
            outputValueActualSchema = resolution.getReaderMatch();
            if (outputValueActualSchema == null) {
              throw new UnsupportedOperationException("dont know how to resolve a " + inputValueActualSchema.getType() + " to " + outputValueDeclaredSchema);
            }
          }

          Object outputElement = deepConvert(
              inputElement,
              inputValueActualSchema,
              outputValueActualSchema,
              context,
              stringRepresentation
          );
          CharSequence outputKey = toString(key, stringRepresentation);
          outputMap.put(outputKey, outputElement);
        }
        return outputMap;

      case UNION:
        inputValueActualSchema = AvroSchemaUtil.resolveUnionBranchOf(inputValue, inputSchema);
        outputValueActualSchema = AvroSchemaUtil.resolveUnionBranchOf(inputValue, outputSchema);
        return deepConvert(inputValue, inputValueActualSchema, outputValueActualSchema, context, stringRepresentation);
    }
    String inputClassName = inputValue == null ? "null" : inputValue.getClass().getName();
    throw new UnsupportedOperationException("dont know how to convert " + inputType + " " + inputValue
            + " (a " + inputClassName + ") into " + outputType);
  }

  /**
   * @param record record
   * @param field (string) field of record
   * @return possible string representations field can accept, in order of preference
   */
  private static List<StringRepresentation> stringRepForSpecificField(SpecificRecord record, Schema.Field field) {
    Class<? extends SpecificRecord> srClass = record.getClass();
    try {
      Field actualField = srClass.getDeclaredField(field.name());
      List<StringRepresentation> classification = classifyStringField(actualField);
      if (classification != null) {
        return classification;
      }
      //its possible the field is a union containing string, in which case the java type will be Object.
      //in this case we try and look for other string fields
      for (Schema.Field otherField : record.getSchema().getFields()) {
        if (otherField == field || otherField.schema().getType() != Schema.Type.STRING) {
          continue;
        }
        Field otherClassField = srClass.getDeclaredField(otherField.name());
        classification = classifyStringField(otherClassField);
        if (classification != null) {
          return classification;
        }
      }

      //no luck, cant determine
      return null;
    } catch (Exception e) {
      throw new IllegalStateException("unable to access " + srClass.getName() + "." + field.name(), e);
    }
  }

  private static List<StringRepresentation> classifyStringField(Field classField) {
    Class<?> type = classField.getType();
    if (String.class.equals(type)) {
      return STRING_ONLY;
    }
    if (CharSequence.class.equals(type)) {
      return UTF8_PREFERRED;
    }
    return null;
  }

  private static StringRepresentation stringRepForGenericField(Schema schema, Schema.Field field) {
    String preferredRepString = field.getProp(HelperConsts.STRING_REPRESENTATION_PROP);
    if (preferredRepString == null) {
      return null;
    }
    return StringRepresentation.valueOf(preferredRepString);
  }

  private static CharSequence toString(CharSequence inputStr, StringRepresentation desired) {
    if (inputStr == null) {
      return null;
    }
    switch (desired) {
      case String:
        return String.valueOf(inputStr);
      case CharSequence:
      case Utf8:
        return new Utf8(String.valueOf(inputStr));
      default:
        throw new IllegalStateException("dont know how to convert to " + desired);
    }
  }

  private static CharSequence toString(byte[] inputBytes, StringRepresentation desired) {
    switch (desired) {
      case String:
        return new String(inputBytes, StandardCharsets.UTF_8);
      case Utf8:
        return new Utf8(inputBytes);
      default:
        throw new IllegalStateException("dont know how to convert to " + desired);
    }
  }

  private static Enum<?> getSpecificEnumSymbol(Class<? extends Enum<?>> enumClass, String symbolStr) {
    try {
      Method valueOf = enumClass.getDeclaredMethod("valueOf", String.class);
      return (Enum<?>) valueOf.invoke(enumClass, symbolStr);
    } catch (Exception e) {
      throw new IllegalStateException("while trying to resolve " + enumClass.getName() + ".valueOf(" + symbolStr + ")", e);
    }
  }
}
