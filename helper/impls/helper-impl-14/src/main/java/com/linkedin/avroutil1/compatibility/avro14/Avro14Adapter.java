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
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.HelperConsts;
import com.linkedin.avroutil1.compatibility.SchemaBuilder;
import com.linkedin.avroutil1.compatibility.SchemaNormalization;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import com.linkedin.avroutil1.compatibility.StringPropertyUtils;
import com.linkedin.avroutil1.compatibility.SchemaValidator;
import com.linkedin.avroutil1.compatibility.SkipDecoder;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import com.linkedin.avroutil1.compatibility.avro14.backports.Avro14DefaultValuesCache;
import com.linkedin.avroutil1.compatibility.avro14.backports.Avro18BufferedBinaryEncoder;
import com.linkedin.avroutil1.compatibility.avro14.codec.BoundedMemoryDecoder;
import com.linkedin.avroutil1.compatibility.avro14.codec.CachedResolvingDecoder;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
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
  public AvroVersion supportedMajorVersion() {
    return AvroVersion.AVRO_1_4;
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
    JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8);
    if (pretty) {
      jsonGenerator.useDefaultPrettyPrinter();
    }
    return new JsonEncoder(schema, jsonGenerator);
  }

  @Override
  public Encoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty, AvroVersion jsonFormat) throws IOException {
    return new CompatibleJsonEncoder(schema, out, pretty, jsonFormat != null && jsonFormat.laterThan(AvroVersion.AVRO_1_4));
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
  public String getDefaultValueAsJsonString(Schema.Field field) {
    JsonNode json = field.defaultValue();
    if (json == null) {
      String fieldStr = field.name() + " type:" + field.schema().getType() + " pos:" + field.pos();
      throw new AvroRuntimeException("Field " + fieldStr + " has no default value");
    }
    return json.toString();
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
  public boolean fieldHasDefault(Schema.Field field) {
    return null != field.defaultValue();
  }

  @Override
  public FieldBuilder newFieldBuilder(Schema.Field other) {
    return new FieldBuilder14(other);
  }

  @Override
  public SchemaBuilder cloneSchema(Schema schema) {
    return new SchemaBuilder14(this, schema);
  }

  @Override
  public String getFieldPropAsJsonString(Schema.Field field, String name) {
    return StringPropertyUtils.getFieldPropAsJsonString(field, name);
  }

  @Override
  public void setFieldPropFromJsonString(Schema.Field field, String name, String value, boolean strict) {
    StringPropertyUtils.setFieldPropFromJsonString(field, name, value, strict);
  }

  @Override
  public String getSchemaPropAsJsonString(Schema schema, String name) {
    return StringPropertyUtils.getSchemaPropAsJsonString(schema, name);
  }

  @Override
  public void setSchemaPropFromJsonString(Schema schema, String name, String value, boolean strict) {
    StringPropertyUtils.setSchemaPropFromJsonString(schema, name, value, strict);
  }

  @Override
  public String toAvsc(Schema schema, boolean pretty) {
    Avro14AvscWriter writer = new Avro14AvscWriter(pretty);
    return writer.toAvsc(schema);
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(
      Collection<Schema> toCompile,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion,
      CodeGenerationConfig config
  ) {
    if (!StringRepresentation.CharSequence.equals(config.getStringRepresentation())) {
      throw new UnsupportedOperationException("generating String fields as " + config.getStringRepresentation() + " unsupported under avro " + supportedMajorVersion());
    }
    if (toCompile == null || toCompile.isEmpty()) {
      return Collections.emptyList();
    }

    Map<String, String> fullNameToAlternativeAvsc = createAlternativeAvscs(toCompile, config);

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
          .peek(code -> code.setAlternativeAvsc(fullNameToAlternativeAvsc.getOrDefault(code.getFullyQualifiedClassName(), null)))
          .collect(Collectors.toList());

      return transform(translated, minSupportedVersion, maxSupportedVersion);
    } catch (UnsupportedOperationException e) {
      throw e; //as-is
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * given schemas about to be code-gen'd and a code-gen config, returns a map of
   * alternative AVSC to use by schema full name.
   * @param toCompile schemas about to be "compiled"
   * @param config configuration
   * @return alternative AVSCs, keyed by schema full name
   */
  private Map<String, String> createAlternativeAvscs(Collection<Schema> toCompile, CodeGenerationConfig config) {
    Set<String> schemasToGenerateBadAvscFor = config.getSchemasToGenerateBadAvscFor();
    if (schemasToGenerateBadAvscFor == null) {
      schemasToGenerateBadAvscFor = Collections.emptySet();
    }
    //we are running under avro 1.4, which will generate (potentially) bad avsc by default
    //hence we only want to have alternative avsc for schemas that would "normally" be impacted
    //by avro-702 but are NOT in schemasToGenerateBadAvscFor
    Map<String, String> fullNameToAlternativeAvsc = new HashMap<>(1); //expected to be small
    for (Schema schema : toCompile) {
      if (!HelperConsts.NAMED_TYPES.contains(schema.getType())) {
        continue;
      }
      String fullName = schema.getFullName();
      if (AvroSchemaUtil.isImpactedByAvro702(schema) && !schemasToGenerateBadAvscFor.contains(fullName)) {
        fullNameToAlternativeAvsc.put(fullName, toAvsc(schema, false));
      }
    }
    return fullNameToAlternativeAvsc;
  }

  private Collection<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroGenerated, AvroVersion minAvro, AvroVersion maxAvro) {
    List<AvroGeneratedSourceCode> transformed = new ArrayList<>(avroGenerated.size());
    for (AvroGeneratedSourceCode generated : avroGenerated) {
      String fixed = CodeTransformations.applyAll(
              generated.getContents(),
              supportedMajorVersion(),
              minAvro,
              maxAvro,
              generated.getAlternativeAvsc()
      );
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
