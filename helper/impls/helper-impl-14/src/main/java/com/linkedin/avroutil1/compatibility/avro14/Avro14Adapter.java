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
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.AvscWriter;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;
import com.linkedin.avroutil1.compatibility.SchemaBuilder;
import com.linkedin.avroutil1.compatibility.SchemaNormalization;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import com.linkedin.avroutil1.compatibility.SkipDecoder;
import com.linkedin.avroutil1.compatibility.StringPropertyUtils;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import com.linkedin.avroutil1.compatibility.avro14.backports.Avro14DefaultValuesCache;
import com.linkedin.avroutil1.compatibility.avro14.backports.Avro18BufferedBinaryEncoder;
import com.linkedin.avroutil1.compatibility.avro14.backports.GenericDatumWriterExt;
import com.linkedin.avroutil1.compatibility.avro14.backports.SpecificDatumWriterExt;
import com.linkedin.avroutil1.compatibility.avro14.codec.AliasAwareSpecificDatumReader;
import com.linkedin.avroutil1.compatibility.avro14.codec.BoundedMemoryDecoder;
import com.linkedin.avroutil1.compatibility.avro14.codec.CachedResolvingDecoder;
import com.linkedin.avroutil1.compatibility.avro14.codec.CompatibleJsonDecoder;
import com.linkedin.avroutil1.compatibility.avro14.codec.CompatibleJsonEncoder;
import com.linkedin.avroutil1.compatibility.backports.ObjectInputToInputStreamAdapter;
import com.linkedin.avroutil1.compatibility.backports.ObjectOutputToOutputStreamAdapter;
import com.linkedin.avroutil1.normalization.AvscWriterPlugin;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;
import org.apache.avro.Avro14SchemaAccessUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Avro14BinaryDecoderAccessUtil;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificCompiler;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Map;
import java.util.Optional;
import java.util.Set;


public class Avro14Adapter implements AvroAdapter {
  private final static Logger LOG = LoggerFactory.getLogger(Avro14Adapter.class);
  private final static DecoderFactory DIRECT_DECODER_FACTORY = new DecoderFactory().configureDirectDecoder(true);
  private final static DecoderFactory BUFFERED_DECODER_FACTORY = DecoderFactory.defaultFactory();

  private final Field fieldAliasesField;
  private final Field fieldPropsField;
  private final Field schemaPropsField;
  private final Method compilerEnqueueMethod;
  private final Method compilerCompileMethod;
  private final Field outputFilePathField;
  private final Field outputFileContentsField;

  public Avro14Adapter() {
    try {
      fieldAliasesField = Schema.Field.class.getDeclaredField("aliases");
      fieldAliasesField.setAccessible(true);
      fieldPropsField = Schema.Field.class.getDeclaredField("props");
      fieldPropsField.setAccessible(true);
      schemaPropsField = Schema.class.getDeclaredField("props");
      schemaPropsField.setAccessible(true);
    } catch (Exception e) {
      throw new IllegalStateException("error initializing adapter", e);
    }
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
  public Encoder newJsonEncoder(Schema schema, OutputStream out, boolean pretty, AvroVersion jsonFormat)
      throws IOException {
    return new CompatibleJsonEncoder(schema, out, pretty,
        jsonFormat != null && jsonFormat.laterThan(AvroVersion.AVRO_1_4));
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
  public <T> SpecificDatumReader<T> newAliasAwareSpecificDatumReader(Schema writer, Class<T> readerClass) {
    Schema readerSchema = AvroSchemaUtil.getDeclaredSchema(readerClass);
    return new AliasAwareSpecificDatumReader<>(writer, readerSchema);
  }

  @Override
  public DatumWriter<?> newGenericDatumWriter(Schema writer, GenericData genericData) {
    return new GenericDatumWriterExt<>(writer, genericData);
  }

  @Override
  public DatumReader<?> newGenericDatumReader(Schema writer, Schema reader, GenericData genericData) {
    // genericData not supported here
    return new GenericDatumReader<>(writer, reader);
  }

  @Override
  public DatumWriter<?> newSpecificDatumWriter(Schema writer, SpecificData specificData) {
    return new SpecificDatumWriterExt<>(writer, specificData);
  }

  @Override
  public DatumReader<?> newSpecificDatumReader(Schema writer, Schema reader, SpecificData specificData) {
    //SDRs in 1.4 do not allow for specifying an instance of SpecificData
    //TODO - get back here if we want to try backporting this to 1.4
    return new SpecificDatumReader<>(writer, reader);
  }

  @Override
  public SchemaParseResult parse(String schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known) {
    SchemaParseResult result = Avro14SchemaAccessUtil.parse(schemaJson, known);
    if (desiredConf != null && !desiredConf.equals(result.getConfigUsed())) {
      //avro 1.4 doesnt validate anything, so if user wants anything stricter we have to do it ourselves
      Schema schema = result.getMainSchema();
      Avro14SchemaValidator validator = new Avro14SchemaValidator(desiredConf, known);
      AvroSchemaUtil.traverseSchema(schema, validator); //will throw on issues
      if (desiredConf.validateNoDanglingContent()) {
        Jackson1Utils.assertNoTrailingContent(schemaJson);
      }
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
  public boolean defaultValuesEqual(Schema.Field a, Schema.Field b, boolean allowLooseNumerics) {
    JsonNode aVal = a.defaultValue();
    JsonNode bVal = b.defaultValue();
    return Jackson1Utils.JsonNodesEqual(aVal, bVal, allowLooseNumerics);
  }

  @Override
  public Set<String> getFieldAliases(Schema.Field field) {
    try {
      @SuppressWarnings("unchecked")
      Set<String> raw = (Set<String>) fieldAliasesField.get(field);
      if (raw == null || raw.isEmpty()) {
        return Collections.emptySet();
      }
      return Collections.unmodifiableSet(raw); //defensive
    } catch (Exception e) {
      throw new IllegalStateException("cant access field aliases", e);
    }
  }

  @Override
  public FieldBuilder newFieldBuilder(Schema.Field other) {
    return new FieldBuilder14(other);
  }

  @Override
  public SchemaBuilder newSchemaBuilder(Schema other) {
    return new SchemaBuilder14(this, other);
  }

  @Override
  public String getFieldPropAsJsonString(Schema.Field field, String name) {
    return StringPropertyUtils.getFieldPropAsJsonString(field, name);
  }

  @Override
  public void setFieldPropFromJsonString(Schema.Field field, String name, String value, boolean strict) {
    StringPropertyUtils.setFieldPropFromJsonString(field, name, value, strict);
  }

  private Map<String, String> getPropsMap(Schema schema) {
    try {
      //noinspection unchecked
      return (Map<String, String>) schemaPropsField.get(schema);
    } catch (Exception e) {
      throw new IllegalStateException("unable to access Schema.Field.props", e);
    }
  }

  private Map<String, String> getPropsMap(Schema.Field field) {
    try {
      //noinspection unchecked
      return (Map<String, String>) fieldPropsField.get(field);
    } catch (Exception e) {
      throw new IllegalStateException("unable to access Schema.Field.props", e);
    }
  }

  @Override
  public boolean sameJsonProperties(Schema.Field a, Schema.Field b, boolean compareStringProps,
      boolean compareNonStringProps, Set<String> jsonPropNamesToIgnore) {
    if (compareNonStringProps) {
      throw new IllegalArgumentException(
          "avro " + supportedMajorVersion() + " does not preserve non-string props and so cannot compare them");
    }
    if (a == null || b == null) {
      return false;
    }
    if (!compareStringProps) {
      return true;
    }
    Map<String, String> unfilteredPropsA = getPropsMap(a);
    Map<String, String> unfilteredPropsB = getPropsMap(b);

    if (jsonPropNamesToIgnore == null) {
      return Objects.equals(unfilteredPropsA, unfilteredPropsB);
    } else {
      Map<String, String> aProps = new HashMap<>();
      Map<String, String> bProps = new HashMap<>();
      for (Map.Entry<String, String> entry : unfilteredPropsA.entrySet()) {
        if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
          aProps.put(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, String> entry : unfilteredPropsB.entrySet()) {
        if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
          bProps.put(entry.getKey(), entry.getValue());
        }
      }
      return Objects.equals(aProps, bProps);
    }
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
  public boolean sameJsonProperties(Schema a, Schema b, boolean compareStringProps, boolean compareNonStringProps,
      Set<String> jsonPropNamesToIgnore) {
    if (compareNonStringProps) {
      throw new IllegalArgumentException(
          "avro " + supportedMajorVersion() + " does not preserve non-string props and so cannot compare them");
    }
    if (a == null || b == null) {
      return false;
    }
    if (!compareStringProps) {
      return true;
    }
    Map<String, String> unfilteredPropsA = getPropsMap(a);
    Map<String, String> unfilteredPropsB = getPropsMap(b);

    if (jsonPropNamesToIgnore == null) {
      return Objects.equals(unfilteredPropsA, unfilteredPropsB);
    } else {
      Map<String, String> aProps = new HashMap<>();
      Map<String, String> bProps = new HashMap<>();
      for (Map.Entry<String, String> entry : unfilteredPropsA.entrySet()) {
        if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
          aProps.put(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, String> entry : unfilteredPropsB.entrySet()) {
        if (!jsonPropNamesToIgnore.contains(entry.getKey())) {
          bProps.put(entry.getKey(), entry.getValue());
        }
      }
      return Objects.equals(aProps, bProps);
    }
  }

  @Override
  public List<String> getAllPropNames(Schema schema) {
    return new ArrayList<>(getPropsMap(schema).keySet());
  }

  @Override
  public List<String> getAllPropNames(Schema.Field field) {
    return new ArrayList<>(getPropsMap(field).keySet());
  }

  @Override
  public String getEnumDefault(Schema s) {
    return s.getProp("default");
  }

  @Override
  public void writeAvsc(Schema schema, AvscGenerationConfig config, Writer writer) {
    boolean useRuntime;
    if (!isRuntimeAvroCapableOf(config)) {
      if (config.isForceUseOfRuntimeAvro()) {
        throw new UnsupportedOperationException(
            "desired configuration " + config + " is forced yet runtime avro " + supportedMajorVersion()
                + " is not capable of it");
      }
      useRuntime = false;
    } else {
      useRuntime = config.isPreferUseOfRuntimeAvro();
    }

    if (useRuntime) {
      try {
        writer.write(schema.toString(config.isPrettyPrint()));
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }
    } else {
      //if the user does not specify do whatever runtime avro would (which for 1.4 means produce bad schema)
      boolean usePre702Logic = config.getRetainPreAvro702Logic().orElse(Boolean.TRUE);
      Avro14AvscWriter avscWriter =
          new Avro14AvscWriter(config.isPrettyPrint(), usePre702Logic, config.isAddAvro702Aliases());
      avscWriter.writeAvsc(schema, writer);
    }
  }

  @Override
  public void writeAvsc(Schema schema, AvscGenerationConfig config, OutputStream stream) {
    boolean useRuntime;
    if (!isRuntimeAvroCapableOf(config)) {
      if (config.isForceUseOfRuntimeAvro()) {
        throw new UnsupportedOperationException(
            "desired configuration " + config + " is forced yet runtime avro " + supportedMajorVersion()
                + " is not capable of it");
      }
      useRuntime = false;
    } else {
      useRuntime = config.isPreferUseOfRuntimeAvro();
    }

    if (useRuntime) {
      try {
        stream.write(schema.toString(config.isPrettyPrint()).getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }
    } else {
      //if the user does not specify do whatever runtime avro would (which for 1.4 means produce bad schema)
      boolean usePre702Logic = config.getRetainPreAvro702Logic().orElse(Boolean.TRUE);
      Avro14AvscWriter avscWriter =
          new Avro14AvscWriter(config.isPrettyPrint(), usePre702Logic, config.isAddAvro702Aliases());
      avscWriter.writeAvsc(schema, stream);
    }
  }

  @Override
  public AvscWriter getAvscWriter(AvscGenerationConfig config, List<AvscWriterPlugin> schemaPlugins) {
    boolean usePre702Logic = config.getRetainPreAvro702Logic().orElse(Boolean.FALSE);
    return new Avro14AvscWriter(config.isPrettyPrint(), usePre702Logic, config.isAddAvro702Aliases(),
        config.retainDefaults, config.retainDocs, config.retainFieldAliases, config.retainNonClaimedProps,
        config.retainSchemaAliases, config.writeNamespaceExplicitly, config.writeRelativeNamespace, config.isLegacy, schemaPlugins);
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion, CodeGenerationConfig config) {
    if (!StringRepresentation.CharSequence.equals(config.getStringRepresentation())) {
      throw new UnsupportedOperationException(
          "generating String fields as " + config.getStringRepresentation() + " unsupported under avro "
              + supportedMajorVersion());
    }
    if (toCompile == null || toCompile.isEmpty()) {
      return Collections.emptyList();
    }

    Map<String, String> fullNameToAlternativeAvsc;
    if (!config.isAvro702HandlingEnabled()) {
      fullNameToAlternativeAvsc = Collections.emptyMap();
    } else {
      fullNameToAlternativeAvsc = createAlternativeAvscs(toCompile, config);
    }

    Iterator<Schema> schemaIter = toCompile.iterator();
    Schema first = schemaIter.next();
    try {
      SpecificCompiler compiler = new SpecificCompiler(first);

      while (schemaIter.hasNext()) {
        compilerEnqueueMethod.invoke(compiler, schemaIter.next());
      }

      Collection<?> outputFiles = (Collection<?>) compilerCompileMethod.invoke(compiler);

      List<AvroGeneratedSourceCode> sourceFiles = new ArrayList<>(outputFiles.size());
      for (Object outputFile : outputFiles) {
        AvroGeneratedSourceCode sourceCode = new AvroGeneratedSourceCode(getPath(outputFile), getContents(outputFile));
        String altAvsc = fullNameToAlternativeAvsc.get(sourceCode.getFullyQualifiedClassName());
        if (altAvsc != null) {
          sourceCode.setAlternativeAvsc(altAvsc);
        }
        sourceFiles.add(sourceCode);
      }

      return transform(sourceFiles, minSupportedVersion, maxSupportedVersion);
    } catch (UnsupportedOperationException e) {
      throw e; //as-is
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Collection<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroGenerated,
      AvroVersion minAvro, AvroVersion maxAvro) {
    List<AvroGeneratedSourceCode> transformed = new ArrayList<>(avroGenerated.size());
    for (AvroGeneratedSourceCode generated : avroGenerated) {
      String fixed = CodeTransformations.applyAll(generated.getContents(), supportedMajorVersion(), minAvro, maxAvro,
          generated.getAlternativeAvsc());
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

  private boolean isRuntimeAvroCapableOf(AvscGenerationConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("config cannot be null");
    }
    if (config.isAddAvro702Aliases()) {
      return false;
    }
    Optional<Boolean> preAvro702Output = config.getRetainPreAvro702Logic();
    //noinspection RedundantIfStatement
    if (preAvro702Output.isPresent() && preAvro702Output.get().equals(Boolean.FALSE)) {
      //avro 1.4 can only do bad avsc
      return false;
    }
    return true;
  }
}
