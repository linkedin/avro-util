/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro110;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.avroutil1.compatibility.AvroAdapter;
import com.linkedin.avroutil1.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.CodeGenerationConfig;
import com.linkedin.avroutil1.compatibility.CodeTransformations;
import com.linkedin.avroutil1.compatibility.ExceptionUtils;
import com.linkedin.avroutil1.compatibility.FieldBuilder;
import com.linkedin.avroutil1.compatibility.Jackson2Utils;
import com.linkedin.avroutil1.compatibility.SchemaBuilder;
import com.linkedin.avroutil1.compatibility.SchemaParseConfiguration;
import com.linkedin.avroutil1.compatibility.SchemaParseResult;
import com.linkedin.avroutil1.compatibility.SkipDecoder;
import com.linkedin.avroutil1.compatibility.StringRepresentation;
import com.linkedin.avroutil1.compatibility.avro110.backports.Avro110DefaultValuesCache;
import com.linkedin.avroutil1.compatibility.avro110.codec.AliasAwareSpecificDatumReader;
import com.linkedin.avroutil1.compatibility.avro110.codec.BoundedMemoryDecoder;
import com.linkedin.avroutil1.compatibility.avro110.codec.CachedResolvingDecoder;
import com.linkedin.avroutil1.compatibility.avro110.codec.CompatibleJsonDecoder;
import com.linkedin.avroutil1.compatibility.avro110.codec.CompatibleJsonEncoder;
import com.linkedin.avroutil1.compatibility.backports.ObjectInputToInputStreamAdapter;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Avro110BinaryDecoderAccessUtil;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.internal.Accessor;
import org.apache.avro.util.internal.JacksonUtils;

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
import java.util.Optional;
import java.util.Set;

public class Avro110Adapter implements AvroAdapter {

    private boolean compilerSupported;
    private Throwable compilerSupportIssue;
    private String compilerSupportMessage;
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

    public Avro110Adapter() {
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
            compilerSupportIssue = e;
            String reason = ExceptionUtils.rootCause(compilerSupportIssue).getMessage();
            compilerSupportMessage = "avro SpecificCompiler class could not be found or instantiated because " + reason
                    + ". please make sure you have a dependency on org.apache.avro:avro-compiler";
            //ignore
        }
    }

    @Override
    public AvroVersion supportedMajorVersion() {
        return AvroVersion.AVRO_1_10;
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
        return Avro110BinaryDecoderAccessUtil.newBinaryDecoder(bytes, offset, length, reuse);
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
    public <T> SpecificDatumReader<T> newAliasAwareSpecificDatumReader(Schema writer, Class<T> readerClass) {
        Schema readerSchema = AvroSchemaUtil.getDeclaredSchema(readerClass);
        return new AliasAwareSpecificDatumReader<>(writer, readerSchema);
    }

    @Override
    public SchemaParseResult parse(String schemaJson, SchemaParseConfiguration desiredConf, Collection<Schema> known) {
        Schema.Parser parser = new Schema.Parser();
        boolean validateNames = true;
        boolean validateDefaults = true;
        boolean validateNumericDefaultValueTypes = false;
        if (desiredConf != null) {
            validateNames = desiredConf.validateNames();
            validateDefaults = desiredConf.validateDefaultValues();
            validateNumericDefaultValueTypes = desiredConf.validateNumericDefaultValueTypes();
        }
        parser.setValidate(validateNames);
        //avro 1.10 default validation also validates numeric types, so if we want to be
        //more nuanced we cant use avro's default value validation
        if (validateDefaults && validateNumericDefaultValueTypes) {
            parser.setValidateDefaults(true);
        } else {
            parser.setValidateDefaults(false);
        }
        if (known != null && !known.isEmpty()) {
            Map<String, Schema> knownByFullName = new HashMap<>(known.size());
            for (Schema s : known) {
                knownByFullName.put(s.getFullName(), s);
            }
            parser.addTypes(knownByFullName);
        }
        Schema mainSchema = parser.parse(schemaJson);
        Map<String, Schema> knownByFullName = parser.getTypes();
        SchemaParseConfiguration configUsed = new SchemaParseConfiguration(validateNames, validateDefaults, validateNumericDefaultValueTypes);
        if (configUsed.validateDefaultValues()) {
            //dont trust avro, also run our own. also we might have told avro not to validate over numerics
            Avro110SchemaValidator validator = new Avro110SchemaValidator(configUsed, known);
            AvroSchemaUtil.traverseSchema(mainSchema, validator);
        }
        //todo - depending on how https://issues.apache.org/jira/browse/AVRO-2742 is settled, may need to use our own validator here
        return new SchemaParseResult(mainSchema, knownByFullName, configUsed);
    }

    @Override
    public String toParsingForm(Schema s) {
        return SchemaNormalization.toParsingForm(s);
    }

    @Override
    public String getDefaultValueAsJsonString(Schema.Field field) {
        JsonNode json = Accessor.defaultValue(field);
        if (json == null) {
            throw new AvroRuntimeException("Field " + field + " has no default value");
        }
        return json.toString();
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
        //always use our cache for the validation
        return Avro110DefaultValuesCache.getDefaultValue(field, false);
    }

    @Override
    public boolean fieldHasDefault(Schema.Field field) {
        return field.hasDefaultValue();
    }

    @Override
    public Set<String> getFieldAliases(Schema.Field field) {
        return field.aliases();
    }

    @Override
    public FieldBuilder newFieldBuilder(Schema.Field other) {
        return new FieldBuilder110(other);
    }

    @Override
    public SchemaBuilder newSchemaBuilder(Schema other) {
        return new SchemaBuilder110(this, other);
    }

    @Override
    public String getFieldPropAsJsonString(Schema.Field field, String name) {
        // Goes from JsonNode to Object to JsonNode (again) to String. Painful, but suffices until somebody complains.
        JsonNode node = JacksonUtils.toJsonNode(field.getObjectProp(name));
        return Jackson2Utils.toJsonString(node);
    }

    @Override
    public void setFieldPropFromJsonString(Schema.Field field, String name, String value, boolean strict) {
        // Goes from String to JsonNode to Object to JsonNode (again). Painful, but suffices until somebody complains.
        JsonNode node = Jackson2Utils.toJsonNode(value, strict);
        field.addProp(name, JacksonUtils.toObject(node));
    }

    @Override
    public String getSchemaPropAsJsonString(Schema schema, String name) {
        // Goes from JsonNode to Object to JsonNode (again) to String. Painful, but suffices until somebody complains.
        JsonNode node = JacksonUtils.toJsonNode(schema.getObjectProp(name));
        return Jackson2Utils.toJsonString(node);
    }

    @Override
    public void setSchemaPropFromJsonString(Schema schema, String name, String value, boolean strict) {
        // Goes from String to JsonNode to Object to JsonNode (again). Painful, but suffices until somebody complains.
        JsonNode node = Jackson2Utils.toJsonNode(value, strict);
        schema.addProp(name, JacksonUtils.toObject(node));
    }

    @Override
    public String getEnumDefault(Schema s) {
        return s.getEnumDefault();
    }

    @Override
    public Schema newEnumSchema(String name, String doc, String namespace, List<String> values, String enumDefault) {
        return Schema.createEnum(name, doc, namespace, values, enumDefault);
    }

    @Override
    public String toAvsc(Schema schema, AvscGenerationConfig config) {
        boolean useRuntime;
        if (!isRuntimeAvroCapableOf(config)) {
            if (config.isForceUseOfRuntimeAvro()) {
                throw new UnsupportedOperationException("desired configuration " + config
                        + " is forced yet runtime avro " + supportedMajorVersion() + " is not capable of it");
            }
            useRuntime = false;
        } else {
            useRuntime = config.isPreferUseOfRuntimeAvro();
        }

        if (useRuntime) {
            return schema.toString(config.isPrettyPrint());
        } else {
            //if the user does not specify do whatever runtime avro would (which for 1.10 means produce correct schema)
            boolean usePre702Logic = config.getRetainPreAvro702Logic().orElse(Boolean.FALSE);
            Avro110AvscWriter writer = new Avro110AvscWriter(
                    config.isPrettyPrint(),
                    usePre702Logic,
                    config.isAddAvro702Aliases()
            );
            return writer.toAvsc(schema);
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
            throw new UnsupportedOperationException(compilerSupportMessage, compilerSupportIssue);
        }
        if (minSupportedVersion.earlierThan(AvroVersion.AVRO_1_6) && !StringRepresentation.CharSequence.equals(config.getStringRepresentation())) {
            throw new IllegalArgumentException("StringRepresentation " + config.getStringRepresentation() + " incompatible with minimum supported avro " + minSupportedVersion);
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

    private boolean isRuntimeAvroCapableOf(AvscGenerationConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        if (config.isAddAvro702Aliases()) {
            return false;
        }
        Optional<Boolean> preAvro702Output = config.getRetainPreAvro702Logic();
        //noinspection RedundantIfStatement
        if (preAvro702Output.isPresent() && preAvro702Output.get().equals(Boolean.TRUE)) {
            //avro 1.10 can only do correct avsc
            return false;
        }
        return true;
    }
}
