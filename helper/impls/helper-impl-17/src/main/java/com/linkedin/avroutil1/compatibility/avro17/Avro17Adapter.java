/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro17;

import com.linkedin.avroutil1.compatibility.AvroAdapter;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import com.linkedin.avroutil1.compatibility.SchemaValidator;
import com.linkedin.avroutil1.compatibility.avro17.backports.Avro17DefaultValuesCache;
import com.linkedin.avroutil1.compatibility.backports.ObjectInputToInputStreamAdapter;
import com.linkedin.avroutil1.compatibility.backports.ObjectOutputToOutputStreamAdapter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Avro17Adapter implements AvroAdapter {
  private final static Logger LOG = LoggerFactory.getLogger(Avro17Adapter.class);

  //compiler-related fields and methods (if the compiler jar is on the classpath)

  private boolean compilerSupported;
  private Constructor<?> specificCompilerCtr;
  private Method compilerEnqueueMethod;
  private Method compilerCompileMethod;
  private Field outputFilePathField;
  private Field outputFileContentsField;
  private Object publicFieldVisibilityEnumInstance;
  private Method setFieldVisibilityMethod;
  private Object charSequenceStringTypeEnumInstance;
  private Method setStringTypeMethod;

  //"optional" methods - things added in later 1.7.* versions that may not exist if we're running with earlier 1.7.*

  /**
   * method {@link org.apache.avro.Schema.Parser#setValidateDefaults(boolean)}.
   */
  private Method setValidateDefaultsMethod;
  /**
   * method {@link SpecificData#getDefaultValue(Schema.Field)}.
   * we use this method's existence to also imply the existence of
   * {@link GenericData#getDefaultValue(Schema.Field)}
   */
  private Method getSpecificDefaultValueMethod;

  public Avro17Adapter() {
    tryInitializeCompilerFields();
  }

  private void tryInitializeCompilerFields() {
    //compiler was moved out into a separate jar in avro 1.5+, so compiler functionality is optional
    try {
      Class<?> compilerClass = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler");
      specificCompilerCtr = compilerClass.getConstructor(Schema.class);
      compilerEnqueueMethod = compilerClass.getDeclaredMethod("enqueue", Schema.class);
      compilerEnqueueMethod.setAccessible(true); //its normally private
      compilerCompileMethod = compilerClass.getDeclaredMethod("compile");
      compilerCompileMethod.setAccessible(true); //package-protected
      Class<?> outputFileClass = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler$OutputFile");
      outputFilePathField = outputFileClass.getDeclaredField("path");
      outputFilePathField.setAccessible(true);
      outputFileContentsField = outputFileClass.getDeclaredField("contents");
      outputFileContentsField.setAccessible(true);
      Class<?> fieldVisibilityEnum = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler$FieldVisibility");
      Field publicVisibilityField = fieldVisibilityEnum.getDeclaredField("PUBLIC");
      publicFieldVisibilityEnumInstance = publicVisibilityField.get(null);
      setFieldVisibilityMethod = compilerClass.getDeclaredMethod("setFieldVisibility", fieldVisibilityEnum);
      Class<?> fieldTypeEnum = Class.forName("org.apache.avro.generic.GenericData$StringType");
      Field charSequenceField = fieldTypeEnum.getDeclaredField("CharSequence");
      charSequenceStringTypeEnumInstance = charSequenceField.get(null);
      setStringTypeMethod = compilerClass.getDeclaredMethod("setStringType", fieldTypeEnum);
      compilerSupported = true;
    } catch (Exception | LinkageError e) {
      //if a class we directly look for above isnt found, we get ClassNotFoundException
      //but if we're missing a transitive dependency we will get NoClassDefFoundError
      compilerSupported = false;
      //ignore
    }

    boolean warned = false;

    try {
      setValidateDefaultsMethod = Schema.Parser.class.getMethod("setValidateDefaults", Boolean.TYPE);
    } catch (Exception | LinkageError e) {
      if (!warned) {
        LOG.warn("you are using an older version of avro 1.7. please consider upgrading to latest 1.7.*");
        warned = true;
      }
      setValidateDefaultsMethod = null;
    }

    try {
      getSpecificDefaultValueMethod = SpecificData.class.getMethod("getDefaultValue", Schema.Field.class);
    } catch (Exception | LinkageError e) {
      if (!warned) {
        LOG.warn("you are using an older version of avro 1.7. please consider upgrading to latest 1.7.*");
        warned = true;
      }
      getSpecificDefaultValueMethod = null;
    }
  }

  @Override
  public BinaryEncoder newBinaryEncoder(OutputStream out, boolean buffered, BinaryEncoder reuse) {
    if (buffered) {
      return EncoderFactory.get().binaryEncoder(out, reuse);
    } else {
      return EncoderFactory.get().directBinaryEncoder(out, reuse);
    }
  }

  @Override
  public BinaryEncoder newBinaryEncoder(ObjectOutput out) {
    return newBinaryEncoder(new ObjectOutputToOutputStreamAdapter(out), false, null);
  }

  @Override
  public BinaryDecoder newBinaryDecoder(InputStream in, boolean buffered, BinaryDecoder reuse) {
    DecoderFactory factory = DecoderFactory.get();
    return buffered ? factory.binaryDecoder(in, reuse) : factory.directBinaryDecoder(in, reuse);
  }

  @Override
  public BinaryDecoder newBinaryDecoder(ObjectInput in) {
    return newBinaryDecoder(new ObjectInputToInputStreamAdapter(in), false, null);
  }

  @Override
  public JsonEncoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty) throws IOException {
    return EncoderFactory.get().jsonEncoder(schema, out, pretty);
  }

  @Override
  public JsonDecoder newJsonDecoder(Schema schema, InputStream in) throws IOException {
    return DecoderFactory.get().jsonDecoder(schema, in);
  }

  @Override
  public JsonDecoder newJsonDecoder(Schema schema, String in) throws IOException {
    return DecoderFactory.get().jsonDecoder(schema, in);
  }

  @Override
  public SchemaParseResult parse(String schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known) {
    Schema.Parser parser = new Schema.Parser();
    boolean validateNames = true;
    boolean validateDefaults = true;
    if (desiredConf != null) {
      validateNames = desiredConf.validateNames();
      validateDefaults = desiredConf.validateDefaultValues();
    }
    SchemaParseConfiguration configUsed = new SchemaParseConfiguration(validateNames, validateDefaults);

    parser.setValidate(validateNames);

    //must check the method exists before trying to call it
    if (setValidateDefaultsMethod != null) {
      parser.setValidateDefaults(validateDefaults);
    }

    if (known != null && !known.isEmpty()) {
      Map<String, Schema> knownByFullName = new HashMap<>(known.size());
      for (Schema s : known) {
        knownByFullName.put(s.getFullName(), s);
      }
      parser.addTypes(knownByFullName);
      parser.addTypes(knownByFullName);
    }
    Schema mainSchema = parser.parse(schemaJson);

    if (validateDefaults && setValidateDefaultsMethod == null) {
      //older avro 1.7 doesnt support validating default values, so we have to do it ourselves
      SchemaValidator validator = new SchemaValidator(configUsed, known);
      AvroSchemaUtil.traverseSchema(mainSchema, validator); //will throw on issues
    }

    Map<String, Schema> knownByFullName = parser.getTypes();
    return new SchemaParseResult(mainSchema, knownByFullName, configUsed);
  }

  @Override
  public String toParsingForm(Schema s) {
    return SchemaNormalization.toParsingForm(s);
  }

  @Override
  public Object newInstance(Class<?> clazz, Schema schema) {
    return SpecificData.newInstance(clazz, schema);
  }

  @Override
  public Object getSpecificDefaultValue(Schema.Field field) {
    if (getSpecificDefaultValueMethod != null) {
      return SpecificData.get().getDefaultValue(field);
    } else {
      //old avro 1.7 - punt to backported code
      return Avro17DefaultValuesCache.getDefaultValue(field, true);
    }
  }

  @Override
  public GenericData.EnumSymbol newEnumSymbol(Schema enumSchema, String enumValue) {
    return new GenericData.EnumSymbol(enumSchema, enumValue);
  }

  @Override
  public GenericData.Fixed newFixedField(Schema fixedSchema) {
    return new GenericData.Fixed(fixedSchema);
  }

  @Override
  public GenericData.Fixed newFixedField(Schema fixedSchema, byte[] contents) {
    return new GenericData.Fixed(fixedSchema, contents);
  }

  @Override
  public Object getGenericDefaultValue(Schema.Field field) {
    if (getSpecificDefaultValueMethod != null) {
      return GenericData.get().getDefaultValue(field);
    } else {
      //old avro 1.7 - punt to backported code
      return Avro17DefaultValuesCache.getDefaultValue(field, false);
    }
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(
      Collection<Schema> toCompile,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion
  ) {
    if (!compilerSupported) {
      throw new UnsupportedOperationException("avro compiler jar was not found on classpath. please make sure you have a dependency on org.apache.avro:avro-compiler");
    }
    if (toCompile == null || toCompile.isEmpty()) {
      return Collections.emptyList();
    }
    Iterator<Schema> schemaIter = toCompile.iterator();
    Schema first = schemaIter.next();
    try {
      //since avro-compiler may not be on the CP, we use pure reflection to deal with the compiler
      Object compiler = specificCompilerCtr.newInstance(first);

      //configure the compiler for broadest compatibility

      //configure string types to generate as CharSequence (because thats the only way 1.4 does this)
      setStringTypeMethod.invoke(compiler, charSequenceStringTypeEnumInstance);
      //make fields public (as avro 1.4 and 1.5 do)
      setFieldVisibilityMethod.invoke(compiler, publicFieldVisibilityEnumInstance);

      while (schemaIter.hasNext()) {
        compilerEnqueueMethod.invoke(compiler, schemaIter.next());
      }

      Collection<?> outputFiles = (Collection<?>) compilerCompileMethod.invoke(compiler);

      List<AvroGeneratedSourceCode> translated = outputFiles.stream()
          .map(o -> new AvroGeneratedSourceCode(getPath(o), getContents(o)))
          .collect(Collectors.toList());

      return transform(translated, minSupportedVersion, maxSupportedVersion);
    } catch (UnsupportedOperationException e) {
      throw e; //as-is
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Collection<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroGenerated, AvroVersion minAvro, AvroVersion maxAvro) {
    List<AvroGeneratedSourceCode> transformed = new ArrayList<>(avroGenerated.size());
    String fixed;
    for (AvroGeneratedSourceCode generated : avroGenerated) {
      fixed = generated.getContents();
      fixed = CodeTransformations.transformFixedClass(fixed, minAvro, maxAvro);
      fixed = CodeTransformations.transformEnumClass(fixed, minAvro, maxAvro);
      fixed = CodeTransformations.transformParseCalls(fixed, AvroVersion.AVRO_1_7, minAvro, maxAvro);
      fixed = CodeTransformations.addGetClassSchemaMethod(fixed, AvroVersion.AVRO_1_7, minAvro, maxAvro);
      fixed = CodeTransformations.removeBuilderSupport(fixed, minAvro, maxAvro);
      fixed = CodeTransformations.removeBinaryMessageCodecSupport(fixed, minAvro, maxAvro);
      fixed = CodeTransformations.removeAvroGeneratedAnnotation(fixed, minAvro, maxAvro);
      fixed = CodeTransformations.transformExternalizableSupport(fixed, minAvro, maxAvro);
      fixed = CodeTransformations.transformCustomCodersSupport(fixed, minAvro, maxAvro);
      transformed.add(new AvroGeneratedSourceCode(generated.getPath(), fixed));
    }
    return transformed;
  }

  private String getPath(Object shouldBeOutputFile) {
    try {
      return (String) outputFilePathField.get(shouldBeOutputFile);
    } catch (Exception e) {
      throw new IllegalStateException("cant extract path from avro OutputFile", e);
    }
  }

  private String getContents(Object shouldBeOutputFile) {
    try {
      return (String) outputFileContentsField.get(shouldBeOutputFile);
    } catch (Exception e) {
      throw new IllegalStateException("cant extract contents from avro OutputFile", e);
    }
  }
}
