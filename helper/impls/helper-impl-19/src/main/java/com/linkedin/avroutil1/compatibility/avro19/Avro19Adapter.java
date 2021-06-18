/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import com.linkedin.avroutil1.compatibility.SkipDecoder;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import com.linkedin.avroutil1.compatibility.avro19.codec.CachedResolvingDecoder;
import com.linkedin.avroutil1.compatibility.avro19.codec.CompatibleJsonDecoder;
import com.linkedin.avroutil1.compatibility.avro19.codec.CompatibleJsonEncoder;
import com.linkedin.avroutil1.compatibility.avro19.codec.BoundedMemoryDecoder;
import com.linkedin.avroutil1.compatibility.backports.ObjectInputToInputStreamAdapter;
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
import org.apache.avro.io.Avro19BinaryDecoderAccessUtil;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.internal.JacksonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Avro19Adapter implements AvroAdapter {
  private final static Logger LOG = LoggerFactory.getLogger(Avro19Adapter.class);

  //doc says thread safe outside config methods
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private boolean compilerSupported;
  private Constructor<?> specificCompilerCtr;
  private Method compilerEnqueueMethod;
  private Method compilerCompileMethod;
  private Field outputFilePathField;
  private Field outputFileContentsField;
  private Object publicFieldVisibilityEnumInstance;
  private Method setFieldVisibilityMethod;
  private Object charSequenceStringTypeEnumInstance;
  private Object javaLangStringTypeEnumInstance;
  private Object utf8TypeEnumInstance;
  private Method setStringTypeMethod;

  public Avro19Adapter() {
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
      Field javaLangStringField = fieldTypeEnum.getDeclaredField("String");
      javaLangStringTypeEnumInstance = javaLangStringField.get(null);
      Field utf8Field = fieldTypeEnum.getDeclaredField("Utf8");
      utf8TypeEnumInstance = utf8Field.get(null);
      setStringTypeMethod = compilerClass.getDeclaredMethod("setStringType", fieldTypeEnum);
      compilerSupported = true;
    } catch (Exception | LinkageError e) {
      //if a class we directly look for above isnt found, we get ClassNotFoundException
      //but if we're missing a transitive dependency we will get NoClassDefFoundError
      compilerSupported = false;
      //ignore
    }
  }

  @Override
  public AvroVersion supportedMajorVersion() {
    return AvroVersion.AVRO_1_9;
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
    return SpecificData.getEncoder(out);
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
  public BinaryDecoder newBinaryDecoder(byte[] bytes, int offset, int length, BinaryDecoder reuse) {
    return Avro19BinaryDecoderAccessUtil.newBinaryDecoder(bytes, offset, length, reuse);
  }

  @Override
  public JsonEncoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty) throws IOException {
    return EncoderFactory.get().jsonEncoder(schema, out, pretty);
  }

  @Override
  public Encoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty, AvroVersion jsonFormat) throws IOException {
    return new CompatibleJsonEncoder(schema, out, pretty, jsonFormat == null || jsonFormat.laterThan(AvroVersion.AVRO_1_4));
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
  public Decoder newCompatibleJsonDecoder(Schema schema, InputStream in) throws IOException {
    return new CompatibleJsonDecoder(schema, in);
  }

  @Override
  public Decoder newCompatibleJsonDecoder(Schema schema, String in) throws IOException {
    return new CompatibleJsonDecoder(schema, in);
  }

  @Override
  public SkipDecoder newCachedResolvingDecoder(Schema writer, Schema reader, Decoder in) throws IOException {
    return new CachedResolvingDecoder(writer, reader, in);
  }

  @Override
  public Decoder newBoundedMemoryDecoder(InputStream in) throws IOException {
    return new BoundedMemoryDecoder(in);
  }

  @Override
  public Decoder newBoundedMemoryDecoder(byte[] data) throws IOException {
    return new BoundedMemoryDecoder(data);
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
    parser.setValidate(validateNames);
    parser.setValidateDefaults(validateDefaults);
    if (known != null && !known.isEmpty()) {
      Map<String, Schema> knownByFullName = new HashMap<>(known.size());
      for (Schema s : known) {
        knownByFullName.put(s.getFullName(), s);
      }
      parser.addTypes(knownByFullName);
    }
    Schema mainSchema = parser.parse(schemaJson);
    Map<String, Schema> knownByFullName = parser.getTypes();
    //todo - depending on how https://issues.apache.org/jira/browse/AVRO-2742 is settled, may need to use our own validator here
    return new SchemaParseResult(mainSchema, knownByFullName, new SchemaParseConfiguration(validateNames, validateDefaults));
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
    return SpecificData.get().getDefaultValue(field);
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
    return GenericData.get().getDefaultValue(field);
  }

  @Override
  public boolean fieldHasDefault(Schema.Field field) {
    return field.hasDefaultValue();
  }

  @Override
  public FieldBuilder cloneSchemaField(Schema.Field field) {
    return new FieldBuilder19(field);
  }

  @Override
  public FieldBuilder newFieldBuilder(String name) {
    return new FieldBuilder19(name);
  }

  @Override
  public String getFieldPropAsJsonString(Schema.Field field, String propName) {
    Object val = field.getObjectProp(propName);
    if (val == null) {
      return null;
    }
    //the following is VERY rube-goldberg-ish, but will do until someone complains
    JsonNode asJsonNode = JacksonUtils.toJsonNode(val);
    try {
      return OBJECT_MAPPER.writeValueAsString(asJsonNode);
    } catch (Exception issue) {
      throw new IllegalStateException("while trying to serialize " + val + " (a " + val.getClass().getName() + ")", issue);
    }
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(
      Collection<Schema> toCompile,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion,
      CodeGenerationConfig config
  ) {
    if (!compilerSupported) {
      throw new UnsupportedOperationException("avro compiler jar was not found on classpath. please make sure you have a dependency on org.apache.avro:avro-compiler");
    }
    if (minSupportedVersion.earlierThan(AvroVersion.AVRO_1_6) && !StringRepresentation.CharSequence.equals(config.getStringRepresentation())) {
      throw new IllegalArgumentException("StringRepresentation " + config.getStringRepresentation() + " incompatible with minimum supported avro " + minSupportedVersion);
    }
    if (toCompile == null || toCompile.isEmpty()) {
      return Collections.emptyList();
    }
    Iterator<Schema> schemaIter = toCompile.iterator();
    Schema first = schemaIter.next();
    try {
      //since avro-compiler may not be on the CP, we use pure reflection to deal with the compiler
      Object compiler = specificCompilerCtr.newInstance(first);

      //configure the compiler

      switch (config.getStringRepresentation()) {
        case CharSequence:
          //this is the (only) way avro 1.4/5 generates code
          setStringTypeMethod.invoke(compiler, charSequenceStringTypeEnumInstance);
          break;
        case String:
          setStringTypeMethod.invoke(compiler, javaLangStringTypeEnumInstance);
          break;
        case Utf8:
          setStringTypeMethod.invoke(compiler, utf8TypeEnumInstance);
          break;
        default:
          throw new IllegalStateException("unhandled StringRepresentation " + config.getStringRepresentation());
      }

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
    for (AvroGeneratedSourceCode generated : avroGenerated) {
      String fixed = CodeTransformations.applyAll(generated.getContents(), supportedMajorVersion(), minAvro, maxAvro);
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
