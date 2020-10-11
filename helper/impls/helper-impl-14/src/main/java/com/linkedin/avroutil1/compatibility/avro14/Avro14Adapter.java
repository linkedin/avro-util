/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.compatibility.AvroAdapter;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import com.linkedin.avroutil1.compatibility.SchemaValidator;
import com.linkedin.avroutil1.compatibility.avro14.backports.Avro14DefaultValuesCache;
import com.linkedin.avroutil1.compatibility.avro14.backports.Avro18BufferedBinaryEncoder;
import com.linkedin.avroutil1.compatibility.SchemaNormalization;
import com.linkedin.avroutil1.compatibility.avro14.codec.CompatibleJsonDecoder;
import com.linkedin.avroutil1.compatibility.avro14.codec.CompatibleJsonEncoder;
import com.linkedin.avroutil1.compatibility.backports.ObjectInputToInputStreamAdapter;
import com.linkedin.avroutil1.compatibility.backports.ObjectOutputToOutputStreamAdapter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Avro14SchemaAccessUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Avro14BinaryDecoderAccessUtil;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Avro14Adapter implements AvroAdapter {
  private final static Logger LOG = LoggerFactory.getLogger(Avro14Adapter.class);
  private final static DecoderFactory DIRECT_DECODER_FACTORY = new DecoderFactory().configureDirectDecoder(true);
  private final static DecoderFactory BUFFERED_DECODER_FACTORY = DecoderFactory.defaultFactory();

  private final Method compilerEnqueueMethod;
  private final Method compilerCompileMethod;
  private final Field outputFilePathField;
  private final Field outputFileContentsField;

  public Avro14Adapter() {
    try {
      compilerEnqueueMethod = SpecificCompiler.class.getDeclaredMethod("enqueue", Schema.class);
      compilerEnqueueMethod.setAccessible(true); //private
      compilerCompileMethod = SpecificCompiler.class.getDeclaredMethod("compile");
      compilerCompileMethod.setAccessible(true); //package-protected
      Class<?> outputFileClass = Class.forName("org.apache.avro.specific.SpecificCompiler$OutputFile");
      outputFilePathField = outputFileClass.getDeclaredField("path");
      outputFilePathField.setAccessible(true);
      outputFileContentsField = outputFileClass.getDeclaredField("contents");
      outputFileContentsField.setAccessible(true);
    } catch (Exception e) {
      //avro 1.4 bundles the compiler into the regular jar, hence this is not expected
      throw new IllegalStateException("error initializing compiler-related fields", e);
    }
  }

  @Override
  public BinaryEncoder newBinaryEncoder(OutputStream out, boolean buffered, BinaryEncoder reuse) {
    if (reuse != null && reuse instanceof Avro18BufferedBinaryEncoder) {
      try {
        reuse.flush();
        reuse.init(out);
      } catch (IOException e) {
        throw new AvroRuntimeException("Failure flushing old output", e);
      }
      return reuse;
    }
    return buffered ? new Avro18BufferedBinaryEncoder(out) : new BinaryEncoder(out);
  }

  @Override
  public BinaryEncoder newBinaryEncoder(ObjectOutput out) {
    return newBinaryEncoder(new ObjectOutputToOutputStreamAdapter(out), false, null);
  }

  @Override
  public BinaryDecoder newBinaryDecoder(InputStream in, boolean buffered, BinaryDecoder reuse) {
    if (buffered) {
      return BUFFERED_DECODER_FACTORY.createBinaryDecoder(in, reuse);
    } else {
      return DIRECT_DECODER_FACTORY.createBinaryDecoder(in, reuse);
    }
  }

  @Override
  public BinaryDecoder newBinaryDecoder(ObjectInput in) {
    return newBinaryDecoder(new ObjectInputToInputStreamAdapter(in), false, null);
  }

  @Override
  public BinaryDecoder newBinaryDecoder(byte[] bytes, int offset, int length, BinaryDecoder reuse) {
    return Avro14BinaryDecoderAccessUtil.newBinaryDecoder(bytes, offset, length, reuse);
  }

  @Override
  public JsonEncoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty) throws IOException {
    return new JsonEncoder(schema, out);
  }

  @Override
  public Encoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty, AvroVersion jsonFormat) throws IOException {
    return new CompatibleJsonEncoder(schema, out, jsonFormat != null && jsonFormat.laterThan(AvroVersion.AVRO_1_4));
  }

  @Override
  public JsonDecoder newJsonDecoder(Schema schema, InputStream in) throws IOException {
    return new JsonDecoder(schema, in);
  }

  @Override
  public JsonDecoder newJsonDecoder(Schema schema, String in) throws IOException {
    return new JsonDecoder(schema, in);
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
  public SchemaParseResult parse(String schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known) {
    SchemaParseResult result = Avro14SchemaAccessUtil.parse(schemaJson, known);
    if (desiredConf != null && !desiredConf.equals(result.getConfigUsed())) {
      //avro 1.4 doesnt validate anything, so if user wants anything stricter we have to do it ourselves
      Schema schema = result.getMainSchema();
      SchemaValidator validator = new SchemaValidator(desiredConf, known);
      AvroSchemaUtil.traverseSchema(schema, validator); //will throw on issues
      return new SchemaParseResult(result.getMainSchema(), result.getAllSchemas(), desiredConf);
    } else {
      return result;
    }
  }

  @Override
  public String toParsingForm(Schema s) {
    return SchemaNormalization.toParsingForm(s);
  }

  @Override
  public Object newInstance(Class<?> clazz, Schema schema) {
    return Avro14SpecificDatumReaderAccessUtil.newInstancePlease(clazz, schema);
  }

  @Override
  public Object getSpecificDefaultValue(Schema.Field field) {
    return Avro14DefaultValuesCache.getDefaultValue(field, true);
  }

  @Override
  public GenericData.EnumSymbol newEnumSymbol(Schema enumSchema, String enumValue) {
    //avro 1.4 doesnt actually have a Schema field in enum literals
    return new GenericData.EnumSymbol(enumValue);
  }

  @Override
  public GenericData.Fixed newFixedField(Schema fixedSchema) {
    return new GenericData.Fixed(fixedSchema);
  }

  @Override
  public GenericData.Fixed newFixedField(Schema fixedSchema, byte[] contents) {
    //avro 1.4 doesnt actually have a Schema field in fixed literals
    return new GenericData.Fixed(contents);
  }

  @Override
  public Object getGenericDefaultValue(Schema.Field field) {
    return Avro14DefaultValuesCache.getDefaultValue(field, false);
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(
      Collection<Schema> toCompile,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion
  ) {
    if (toCompile == null || toCompile.isEmpty()) {
      return Collections.emptyList();
    }
    Iterator<Schema> schemaIter = toCompile.iterator();
    Schema first = schemaIter.next();
    try {
      SpecificCompiler compiler = new SpecificCompiler(first);

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
      fixed = CodeTransformations.transformParseCalls(fixed, AvroVersion.AVRO_1_4, minAvro, maxAvro);
      fixed = CodeTransformations.addGetClassSchemaMethod(fixed, AvroVersion.AVRO_1_4, minAvro, maxAvro);
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
