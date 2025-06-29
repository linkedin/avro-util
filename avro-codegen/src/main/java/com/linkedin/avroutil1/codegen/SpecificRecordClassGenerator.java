/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avroutil1.compatibility.AvroRecordUtil;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CompatibleSpecificRecordBuilderBase;
import com.linkedin.avroutil1.compatibility.HelperConsts;
import com.linkedin.avroutil1.compatibility.SourceCodeUtils;
import com.linkedin.avroutil1.compatibility.StringUtils;
import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroJavaStringRepresentation;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.writer.avsc.AvscSchemaWriter;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * generates java classes out of avro schemas.
 */
public class SpecificRecordClassGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpecificRecordClassGenerator.class);

  private static class Counter {

    private int _counter = -1;

    void increment() {
      _counter++;
    }

    int get() {
      return _counter;
    }

    void reset() {
      _counter = -1;
    }
  }

  /***
   * Generates Java class for top level schema.
   *
   * @return Java class of top level schema
   *
   * @throws ClassNotFoundException
   */
  public JavaFile generateSpecificClass(AvroNamedSchema topLevelSchema,
      SpecificRecordGenerationConfig config) throws ClassNotFoundException {
    if (topLevelSchema == null) {
      throw new IllegalArgumentException("topLevelSchema required");
    }
    AvroType type = topLevelSchema.type();
    switch (type) {
      case ENUM:
        return generateSpecificEnum((AvroEnumSchema) topLevelSchema, config);
      case FIXED:
        return generateSpecificFixed((AvroFixedSchema) topLevelSchema, config);
      case RECORD:
        return generateSpecificRecord((AvroRecordSchema) topLevelSchema, config);
      default:
        throw new IllegalArgumentException("cant generate java class for " + type);
    }
  }

  /**
   * Generates Java classes for top level schemas and all internally defined types, excludes references.
   * Not used during codegen. Uncomment for local testing
   *
   * @return List of Java files
   * @throws ClassNotFoundException
   */
//  public List<JavaFileObject> generateSpecificClassWithInternalTypes(AvroNamedSchema topLevelSchema,
//      SpecificRecordGenerationConfig config) throws ClassNotFoundException {
//
//    AvroType type = topLevelSchema.type();
//    switch (type) {
//      case ENUM:
//      case FIXED:
//        return Arrays.asList(generateSpecificClass(topLevelSchema, config));
//      case RECORD:
//        List<JavaFileObject> namedSchemaFiles = new ArrayList<>();
//        populateJavaFilesOfInnerNamedSchemasFromRecord((AvroRecordSchema) topLevelSchema, config, namedSchemaFiles);
//        namedSchemaFiles.add(generateSpecificRecord((AvroRecordSchema) topLevelSchema, config));
//        return namedSchemaFiles;
//      default:
//        throw new IllegalArgumentException("cant generate java class for " + type);
//    }
//  }

  /***
   * Runs through internally defined schemas and generates their file objects
   *
   */
  private void populateJavaFilesOfInnerNamedSchemasFromRecord(AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config, List<JavaFile> namedSchemaFiles) throws ClassNotFoundException {

    HashSet<String> visitedSchemasFullNames = new HashSet<>();
    Queue<AvroSchema> schemaQueue = recordSchema.getFields()
        .stream()
        .filter(field -> field.getSchemaOrRef().getRef() == null)
        .map(AvroSchemaField::getSchema)
        .collect(Collectors.toCollection(LinkedList::new));

    while (!schemaQueue.isEmpty()) {
      AvroSchema fieldSchema = schemaQueue.poll();
      if (fieldSchema instanceof AvroNamedSchema) {
        String fieldFullName = ((AvroNamedSchema) fieldSchema).getFullName();
        if (visitedSchemasFullNames.contains(fieldFullName)) {
          continue;
        } else {
          visitedSchemasFullNames.add(fieldFullName);
        }
      }
      switch (fieldSchema.type()) {
        case RECORD:
          // record's inner fields can also be named types. Add them to the queue
          schemaQueue.addAll(((AvroRecordSchema) fieldSchema).getFields()
              .stream()
              .filter(field -> field.getSchemaOrRef().getRef() == null)
              .map(AvroSchemaField::getSchema)
              .collect(Collectors.toList()));
          namedSchemaFiles.add(generateSpecificRecord((AvroRecordSchema) fieldSchema, config));
          break;
        case FIXED:
          namedSchemaFiles.add(generateSpecificFixed((AvroFixedSchema) fieldSchema, config));
          break;
        case UNION:
          // add union members to fields queue
          ((AvroUnionSchema) fieldSchema).getTypes().forEach(unionMember -> {
            if (AvroType.NULL != unionMember.getSchema().type() && unionMember.getRef() == null) {
              schemaQueue.add(unionMember.getSchema());
            }
          });
          break;
        case ENUM:
          namedSchemaFiles.add(generateSpecificEnum((AvroEnumSchema) fieldSchema, config));
          break;
        case MAP:
          schemaQueue.add(((AvroMapSchema) fieldSchema).getValueSchema());
          break;
        case ARRAY:
          schemaQueue.add(((AvroArraySchema) fieldSchema).getValueSchema());
          break;
      }
    }
  }


  protected JavaFile generateSpecificEnum(AvroEnumSchema enumSchema, SpecificRecordGenerationConfig config) {
    //public enum
    TypeSpec.Builder classBuilder = TypeSpec.enumBuilder(enumSchema.getSimpleName());
    classBuilder.addModifiers(Modifier.PUBLIC);

    //add class-level doc from schema doc
    //file-level (top of file) comment is added to the file object later
    String doc = enumSchema.getDoc();
    if (doc != null && !doc.isEmpty()) {
      classBuilder.addJavadoc(doc);
    }

    //add all symbols
    for (String symbolStr : enumSchema.getSymbols()) {
      classBuilder.addEnumConstant(symbolStr);
    }

    //add public final static SCHEMA$
    addSchema$ToGeneratedClass(classBuilder, enumSchema);

    //add getClassSchema method
    classBuilder.addMethod(MethodSpec.methodBuilder("getClassSchema")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    //add getSchema method
    classBuilder.addMethod(MethodSpec.methodBuilder("getSchema")
        .addModifiers(Modifier.PUBLIC)
        .returns(SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    //create file object
    TypeSpec classSpec = classBuilder.build();

    return JavaFile.builder(enumSchema.getNamespace(), classSpec)
        .skipJavaLangImports(false) //no imports
        .addFileComment(SpecificRecordGeneratorUtil.AVRO_GEN_COMMENT)
        .build();
  }


  protected JavaFile generateSpecificFixed(AvroFixedSchema fixedSchema, SpecificRecordGenerationConfig config)
      throws ClassNotFoundException {
    //public class
    TypeSpec.Builder classBuilder = TypeSpec.classBuilder(fixedSchema.getSimpleName());
    classBuilder.addModifiers(Modifier.PUBLIC);

    //add class-level doc from schema doc
    //file-level (top of file) comment is added to the file object later
    String doc = fixedSchema.getDoc();
    if (doc != null && !doc.isEmpty()) {
      classBuilder.addJavadoc(doc);
    }

    //add public final static SCHEMA$
    addSchema$ToGeneratedClass(classBuilder, fixedSchema);

    // extends SpecificFixed from avro
    classBuilder.superclass(SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_FIXED);

    classBuilder.addSuperinterface(java.io.Externalizable.class);

    // no args constructor
    classBuilder.addMethod(
        MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).addStatement("super()").build());

    // constructor with bytes args
    classBuilder.addMethod(
        MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(byte[].class, "bytes")
            .addStatement("super()")
            .addStatement("bytes(bytes)")
            .build()
    );

    addCommonClassComponents(classBuilder);

    //add size annotation to class
    addAndInitializeSizeFieldToClass(classBuilder, fixedSchema);

    //create file object
    TypeSpec classSpec = classBuilder.build();

    return JavaFile.builder(fixedSchema.getNamespace(), classSpec)
        .skipJavaLangImports(false) //no imports
        .addFileComment(SpecificRecordGeneratorUtil.AVRO_GEN_COMMENT)
        .build();
  }

  /***
   * Adds getClassSchema, getSchema, DatumReader, DatumWriter
   */
  private void addCommonClassComponents(TypeSpec.Builder classBuilder) {
    //add getClassSchema method
    classBuilder.addMethod(MethodSpec.methodBuilder("getClassSchema")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    //add getSchema method
    classBuilder.addMethod(MethodSpec.methodBuilder("getSchema")
        .addModifiers(Modifier.PUBLIC)
        .returns(SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    //read external
    classBuilder.addField(
        FieldSpec.builder(SpecificRecordGeneratorUtil.CLASSNAME_DATUM_READER, "READER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(CodeBlock.of("new $T($L)", SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_DATUM_READER, "SCHEMA$"))
            .build());

    MethodSpec.Builder readExternalBuilder = MethodSpec.methodBuilder("readExternal")
        .addException(IOException.class)
        .addParameter(ObjectInput.class, "in")
        .addModifiers(Modifier.PUBLIC)
        .addCode(CodeBlock.builder()
            .addStatement(
                "$L.read(this, com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.newBinaryDecoder(in))",
                "READER$")
            .build());

    // write external
    classBuilder.addField(
        FieldSpec.builder(SpecificRecordGeneratorUtil.CLASSNAME_DATUM_WRITER, "WRITER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(CodeBlock.of("new $T($L)", SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_DATUM_WRITER, "SCHEMA$"))
            .build());

    MethodSpec.Builder writeExternalBuilder = MethodSpec
        .methodBuilder("writeExternal")
        .addException(IOException.class)
        .addParameter(ObjectOutput.class, "out")
        .addModifiers(Modifier.PUBLIC)
        .addCode(CodeBlock
            .builder()
            .addStatement(
                "$L.write(this, com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.newBinaryEncoder(out))",
                "WRITER$")
            .build());

    classBuilder.addMethod(readExternalBuilder.build());
    classBuilder.addMethod(writeExternalBuilder.build());
  }

  protected JavaFile generateSpecificRecord(AvroRecordSchema recordSchema, SpecificRecordGenerationConfig config)
      throws ClassNotFoundException {

    Counter sizeValCounter = new Counter();

    // Default to broad compatibility config if null
    if(config == null) {
      config = SpecificRecordGenerationConfig.BROAD_COMPATIBILITY;
    }

    //public class
    TypeSpec.Builder classBuilder = TypeSpec.classBuilder(recordSchema.getSimpleName());
    classBuilder.addModifiers(Modifier.PUBLIC);

    //implements
    classBuilder.addSuperinterface(SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_RECORD);

    // extends
    classBuilder.superclass(SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_RECORD_BASE);

    //add class-level doc from schema doc
    //file-level (top of file) comment is added to the file object later
    String doc = recordSchema.getDoc();
    if (doc != null && !doc.isEmpty()) {
      doc = replaceSingleDollarSignWithDouble(doc);
      classBuilder.addJavadoc(doc);
    }

    //MODEL$
    if(config.getMinimumSupportedAvroVersion().laterThan(AvroVersion.AVRO_1_7)) {
      // MODEL$ as new instance of SpecificData()
      classBuilder.addField(
          FieldSpec.builder(SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_DATA, "MODEL$", Modifier.PRIVATE,
              Modifier.STATIC)
              .initializer(CodeBlock.of("new $T()", SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_DATA))
              .build());
    } else {
      classBuilder.addField(
          FieldSpec.builder(SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_DATA, "MODEL$", Modifier.PRIVATE,
              Modifier.STATIC)
              .initializer(CodeBlock.of("$T.get()", SpecificRecordGeneratorUtil.CLASSNAME_SPECIFIC_DATA))
              .build());
    }

    // serialVersionUID
    classBuilder.addField(
        FieldSpec.builder(long.class, "serialVersionUID", Modifier.PRIVATE,
            Modifier.STATIC, Modifier.FINAL)
            .initializer("1L")
            .build());

    //add public final static SCHEMA$
    addSchema$ToGeneratedClass(classBuilder, recordSchema);
    classBuilder.addMethod(MethodSpec.methodBuilder("getSchema")
        .addModifiers(Modifier.PUBLIC)
        .addAnnotation(Override.class)
        .returns(SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    classBuilder.addMethod(MethodSpec.methodBuilder("getClassSchema")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    //READER$
    ParameterizedTypeName datumReaderType = ParameterizedTypeName.get(
        SpecificRecordGeneratorUtil.CLASSNAME_DATUM_READER,
        TypeVariableName.get(recordSchema.getSimpleName())
    );
    classBuilder.addField(
        FieldSpec.builder(datumReaderType, "READER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(
                CodeBlock.of("com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.newSpecificDatumReader($L, $L, $L)", "SCHEMA$", "SCHEMA$", "MODEL$")
            )
            .build()
    );

    //readExternal()
    MethodSpec.Builder readExternalBuilder = MethodSpec.methodBuilder("readExternal")
        .addException(IOException.class)
        .addParameter(java.io.ObjectInput.class, "in")
        .addModifiers(Modifier.PUBLIC)
        .addCode(CodeBlock.builder().addStatement("$L.read(this, com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.newBinaryDecoder(in))", "READER$").build());
    //no @Overrides annotation since under old avro the parent class is not externalizable
    //TODO - add this is min avro version is high enough
    //readExternalBuilder.addAnnotation(Override.class);
    classBuilder.addMethod(readExternalBuilder.build());

    //WRITER$
    ParameterizedTypeName datumWriterType = ParameterizedTypeName.get(
        SpecificRecordGeneratorUtil.CLASSNAME_DATUM_WRITER,
        TypeVariableName.get(recordSchema.getSimpleName())
    );
    classBuilder.addField(
        FieldSpec.builder(datumWriterType, "WRITER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(
                CodeBlock.of("com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.newSpecificDatumWriter($L, $L)", "SCHEMA$", "MODEL$")
            )
            .build());

    //writeExternal()
    MethodSpec.Builder writeExternalBuilder = MethodSpec
        .methodBuilder("writeExternal")
        .addException(IOException.class)
        .addParameter(java.io.ObjectOutput.class, "out")
        .addModifiers(Modifier.PUBLIC)
        .addCode(CodeBlock
            .builder()
            .addStatement("$L.write(this, com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.newBinaryEncoder(out))", "WRITER$")
            .build());
    //no @Overrides annotation since under old avro the parent class is not externalizable
    //TODO - add this is min avro version is high enough
    //writeExternalBuilder.addAnnotation(Override.class);
    classBuilder.addMethod(writeExternalBuilder.build());

    // add no arg constructor
    classBuilder.addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build());

    if (!recordSchema.getFields().isEmpty()) {
      // add all arg constructor if #args < 254
      addAllArgsConstructor(recordSchema, config.getDefaultMethodStringRepresentation(), classBuilder, !config.isUtf8EncodingEnabled());

      // Add numeric conversion constructors. This will add constructor using Integer param for long/Long fields, and
      //  Long param for int/Integer fields. Note that this will not create all constructor permutations
      addNumericConversionConstructors(recordSchema, config, classBuilder);

      if (config.isUtf8EncodingEnabled() && SpecificRecordGeneratorUtil.recordHasSimpleStringField(recordSchema)) {
        addAllArgsConstructor(recordSchema,
            config.getDefaultMethodStringRepresentation().equals(AvroJavaStringRepresentation.STRING)
                ? AvroJavaStringRepresentation.CHAR_SEQUENCE : AvroJavaStringRepresentation.STRING,
            classBuilder, !config.isUtf8EncodingEnabled());
      }

      // Add public/private fields
      Modifier accessModifier = (config.hasPublicFields())? Modifier.PUBLIC : Modifier.PRIVATE;
      for (AvroSchemaField field : recordSchema.getFields()) {
        FieldSpec.Builder fieldBuilder = getFieldSpecBuilder(field, config).addModifiers(accessModifier);
        if(config.hasPublicFields()) {
          fieldBuilder.addAnnotation(Deprecated.class);
          fieldBuilder.addJavadoc("@deprecated public fields are deprecated. Please use setters/getters.");
        }
        classBuilder.addField(fieldBuilder.build());

        //getters
        classBuilder.addMethod(getGetterMethodSpec(field, config));

        // setters
        classBuilder.addMethod(getSetterMethodSpec(field, config));
        MethodSpec overloadedSetterIfString = getOverloadedSetterSpecIfStringField(field, config);
        if(config.isUtf8EncodingEnabled() && overloadedSetterIfString != null) {
          classBuilder.addMethod(getOverloadedSetterSpecIfStringField(field, config));
        }
        MethodSpec overloadedSetterIfIntOrLong = getOverloadedSetterSpecIfIntOrLongField(field, config);
        if (overloadedSetterIfIntOrLong != null) {
          classBuilder.addMethod(overloadedSetterIfIntOrLong);
        }
      }
    }

    // Add get method by index
    addGetByIndexMethod(classBuilder, recordSchema, config);

    //Add put method by index
    addPutByIndexMethod(classBuilder, recordSchema, config);

    classBuilder.addMethod(
        MethodSpec.methodBuilder("hasCustomCoders")
            .addModifiers(Modifier.PROTECTED)
            .returns(boolean.class)
            .addStatement("return $L", true)
            .build());

    // customEncode
    MethodSpec.Builder customEncodeBuilder = MethodSpec
        .methodBuilder("customEncode")
        .addParameter(SpecificRecordGeneratorUtil.CLASSNAME_ENCODER, "out")
        .addException(IOException.class)
        .addModifiers(Modifier.PUBLIC);
    addCustomEncodeMethod(customEncodeBuilder, recordSchema, config, sizeValCounter);
    classBuilder.addMethod(customEncodeBuilder.build());

    //customDecode
    MethodSpec.Builder customDecodeBuilder = MethodSpec
        .methodBuilder("customDecode")
        .addParameter(SpecificRecordGeneratorUtil.CLASSNAME_RESOLVING_DECODER, "in")
        .addException(IOException.class)
        .addModifiers(Modifier.PUBLIC);
    addCustomDecodeMethod(customDecodeBuilder, recordSchema, config, classBuilder, sizeValCounter);
    classBuilder.addMethod(customDecodeBuilder.build());

    // Builder
    TypeSpec.Builder recordBuilder = TypeSpec.classBuilder("Builder");
    recordBuilder.addModifiers(Modifier.PUBLIC, Modifier.STATIC);
    try {
      populateBuilderClassBuilder(recordBuilder, recordSchema, config);
      //new Builder methods
      classBuilder.addMethods(getNewBuilderMethods(recordSchema));
      classBuilder.addType(recordBuilder.build());
    } catch (ClassNotFoundException e) {
      throw new ClassNotFoundException("Exception while creating Builder: %s", e);
    }

    //create file object
    TypeSpec classSpec = classBuilder.build();
    return JavaFile.builder(recordSchema.getNamespace(), classSpec)
        .skipJavaLangImports(false) //no imports
        .addFileComment(SpecificRecordGeneratorUtil.AVRO_GEN_COMMENT).build();
  }

  private void addAllArgsConstructor(AvroRecordSchema recordSchema,
      AvroJavaStringRepresentation defaultMethodStringRepresentation, TypeSpec.Builder classBuilder, boolean disableStringTransform) {
    if(recordSchema.getFields().size() < 254) {
      MethodSpec.Builder allArgsConstructorBuilder = MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC);
      for (AvroSchemaField field : recordSchema.getFields()) {
        //if declared schema, use fully qualified class (no import)
        String escapedFieldName = getFieldNameWithSuffix(field);
        allArgsConstructorBuilder.addParameter(getParameterSpecForField(field, defaultMethodStringRepresentation));
        if(!disableStringTransform &&  SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, field.getSchema())) {
          allArgsConstructorBuilder.addStatement(
              "this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8($1L)",
              escapedFieldName);
        } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(field.getSchema(), disableStringTransform)) {
          allArgsConstructorBuilder.addStatement(
              "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8($1L)",
              escapedFieldName);
        } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(field.getSchema(), disableStringTransform)) {
          allArgsConstructorBuilder.addStatement(
              "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8($1L)",
              escapedFieldName);
        } else if (field.getSchema() != null && AvroType.UNION.equals(field.getSchema().type())
            && !SpecificRecordGeneratorUtil.isSingleTypeNullableUnionSchema(field.getSchema())) {

          allArgsConstructorBuilder.beginControlFlow("if ($1L == null)", escapedFieldName)
              .addStatement("this.$1L = null", escapedFieldName)
              .endControlFlow();

          // if union might contain string value in runtime
          for (SchemaOrRef unionMemberSchema : ((AvroUnionSchema) field.getSchema()).getTypes()) {
            if (!disableStringTransform && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, unionMemberSchema.getSchema())) {
              allArgsConstructorBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName,
                      CharSequence.class)
                  .addStatement("this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8($1L)",
                      escapedFieldName)
                  .endControlFlow();
            } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(
                unionMemberSchema.getSchema(), disableStringTransform)) {
              allArgsConstructorBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, List.class)
                  .addStatement(
                      "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8($1L)",
                      escapedFieldName)
                  .endControlFlow();
            } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(unionMemberSchema.getSchema(), disableStringTransform)) {
              allArgsConstructorBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, Map.class)
                  .addStatement(
                      "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8($1L)",
                      escapedFieldName)
                  .endControlFlow();
            }
          }

          allArgsConstructorBuilder.beginControlFlow("else")
              .addStatement("this.$1L = $1L", escapedFieldName)
              .endControlFlow();
        } else {
          allArgsConstructorBuilder.addStatement("this.$1L = $1L", escapedFieldName);
        }
      }

      //CharSequence constructors are deprecated in favor of String constructors
      if (defaultMethodStringRepresentation.equals(AvroJavaStringRepresentation.CHAR_SEQUENCE)
          && SpecificRecordGeneratorUtil.recordHasSimpleStringField(recordSchema)) {
        allArgsConstructorBuilder.addAnnotation(Deprecated.class);
      }

      classBuilder.addMethod(allArgsConstructorBuilder.build());
    }
  }

  /**
   * Adds a constructor that performs numeric conversions for {@link Integer} and {@link Long} fields.
   * @param recordSchema The Avro record schema.
   * @param config The {@link SpecificRecordGenerationConfig}
   * @param classBuilder The {@link TypeSpec.Builder} for the generated class.
   */
  private void addNumericConversionConstructors(AvroRecordSchema recordSchema, SpecificRecordGenerationConfig config,
      TypeSpec.Builder classBuilder) {
    // Only proceed if we have fewer than 254 fields (Java method parameter limit)
    if (recordSchema.getFields().size() >= 254) {
      return;
    }

    // If the record has any Integer/int or Long/long fields
    // Add a constructor that handles both types of conversions
    if (hasIntOrLongField(recordSchema)) {
      addMixedNumericConversionConstructor(recordSchema, config, classBuilder);
    }
  }

  /**
   * Checks if the record schema has int or long field.
   *
   * @param recordSchema The record schema to check
   * @return True if the schema has at least one int or long field
   */
  private boolean hasIntOrLongField(AvroRecordSchema recordSchema) {
    for (AvroSchemaField field : recordSchema.getFields()) {
      // Check if field is long or [null, long]
      if (SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.LONG, field.getSchema())
          || SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.INT, field.getSchema())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Adds a constructor that handles both Integer-to-Long and Long-to-Integer conversions simultaneously
   *
   * @param recordSchema The record schema
   * @param config The {@link SpecificRecordGenerationConfig}
   * @param classBuilder The class builder to add the constructor to
   */
  private void addMixedNumericConversionConstructor(AvroRecordSchema recordSchema, SpecificRecordGenerationConfig config,
      TypeSpec.Builder classBuilder) {
    MethodSpec.Builder constructorBuilder = MethodSpec.constructorBuilder()
        .addModifiers(Modifier.PUBLIC);

    for (AvroSchemaField field : recordSchema.getFields()) {
      String escapedFieldName = getFieldNameWithSuffix(field);

      if (SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.LONG, field.getSchema())
          || SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.INT, field.getSchema())) {

        // Get the class from the schema
        Class<?> fieldClass =
            SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(field.getSchemaOrRef().getSchema().type(),
                config.getDefaultMethodStringRepresentation(), false);

        if(fieldClass != null ) {
          if (fieldClass.equals(long.class)) {
            // For primitive long, throw exception on null or use default value
            constructorBuilder
                .addParameter(ParameterSpec.builder(Integer.class, escapedFieldName).build())
                .beginControlFlow("if ($L != null)", escapedFieldName)
                .addStatement("this.$1L = $1L.longValue()", escapedFieldName)
                .nextControlFlow("else")
                .addStatement("throw new org.apache.avro.AvroRuntimeException(\"$L cannot be set to null\")", escapedFieldName)
                .endControlFlow();
          } else if (fieldClass.equals(int.class)) {
            // For primitive int, throw exception on null or use default value
            constructorBuilder
                .addParameter(ParameterSpec.builder(Long.class, escapedFieldName).build())
                .beginControlFlow("if ($L != null)", escapedFieldName)
                .beginControlFlow("if ($1L <= Integer.MAX_VALUE && $1L >= Integer.MIN_VALUE)", escapedFieldName)
                .addStatement("this.$1L = $1L.intValue()", escapedFieldName)
                .nextControlFlow("else")
                .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value \" + $L + \" cannot be cast to int\")", escapedFieldName)
                .endControlFlow()
                .nextControlFlow("else")
                .addStatement("throw new org.apache.avro.AvroRuntimeException(\"$L cannot be set to null\")", escapedFieldName)
                .endControlFlow();
          }
        } else if (field.getSchema() != null && field.getSchema().type().equals(AvroType.UNION)) {
          TypeName typeName = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
              field.getSchemaOrRef().getSchema().type(), true,
              config.getDefaultMethodStringRepresentation());
          if (typeName.equals(ClassName.get(Long.class))) {
            constructorBuilder
                .addParameter(ParameterSpec.builder(Integer.class, escapedFieldName).build())
                .beginControlFlow("if ($L != null)", escapedFieldName)
                .addStatement("this.$1L = Long.valueOf($1L)", escapedFieldName)
                .nextControlFlow("else")
                .addStatement("this.$L = null", escapedFieldName)
                .endControlFlow();
          } else if (typeName.equals(ClassName.get(Integer.class))) {
            constructorBuilder
                .addParameter(ParameterSpec.builder(Long.class, escapedFieldName).build())
                .beginControlFlow("if ($L != null)", escapedFieldName)
                .beginControlFlow("if ($1L <= Integer.MAX_VALUE && $1L >= Integer.MIN_VALUE)", escapedFieldName)
                .addStatement("this.$1L = $1L.intValue()", escapedFieldName)
                .nextControlFlow("else")
                .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value \" + $L + \" cannot be cast to Integer\")", escapedFieldName)
                .endControlFlow()
                .nextControlFlow("else")
                .addStatement("this.$L = null", escapedFieldName)
                .endControlFlow();
          }
        }
      } else  {
        // For other fields, use their normal type
        constructorBuilder.addParameter(
            getParameterSpecForField(field, AvroJavaStringRepresentation.STRING));
        constructorBuilder.addStatement("this.$1L = $1L", escapedFieldName);

      }
    }
    classBuilder.addMethod(constructorBuilder.build());
  }

  private String replaceSingleDollarSignWithDouble(String str) {
    if (str != null && str.contains("$")) {
      str = SpecificRecordGeneratorUtil.SINGLE_DOLLAR_SIGN_REGEX.matcher(str).replaceAll("\\$\\$");
    }
    return str;
  }

  private void populateBuilderClassBuilder(TypeSpec.Builder recordBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) throws ClassNotFoundException {
    recordBuilder.superclass(ClassName.get(CompatibleSpecificRecordBuilderBase.class));
    CodeBlock.Builder otherBuilderConstructorFromRecordBlockBuilder = CodeBlock.builder();
    CodeBlock.Builder otherBuilderConstructorFromOtherBuilderBlockBuilder = CodeBlock.builder();
    CodeBlock.Builder buildMethodCodeBlockBuilder = CodeBlock.builder()
        .beginControlFlow("try")
        .addStatement("$1L record = new $1L()", recordSchema.getName().getSimpleName());

    List<MethodSpec> accessorMethodSpecs = new ArrayList<>();
    int fieldIndex = 0;
    // All private fields, string representation same as method
    for (AvroSchemaField field : recordSchema.getFields()) {
      FieldSpec.Builder fieldBuilder;
      String escapedFieldName = getFieldNameWithSuffix(field);
      AvroSchema fieldSchema = field.getSchemaOrRef().getSchema();
      AvroType fieldAvroType = fieldSchema.type();
      Class<?> fieldClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(fieldAvroType,
          config.getDefaultMethodStringRepresentation(), false);
      TypeName fieldType = SpecificRecordGeneratorUtil.getTypeName(field.getSchema(), fieldAvroType, true,
          config.getDefaultMethodStringRepresentation());

      if (fieldClass != null) {
        fieldBuilder = FieldSpec.builder(fieldClass, escapedFieldName, Modifier.PRIVATE);
        if(config.isUtf8EncodingEnabled() && (AvroType.STRING.equals(fieldSchema.type()) || SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, field.getSchema()))) {
          buildMethodCodeBlockBuilder.addStatement(
              "record.$1L = fieldSetFlags()[$2L] ? "
                  + "com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8(this.$1L) : "
                  + "($3T) com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8(com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L]))",
              escapedFieldName, fieldIndex,
              SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(fieldAvroType,
                  config.getDefaultFieldStringRepresentation(), false));
        } else {
          buildMethodCodeBlockBuilder.addStatement(
              "record.$1L = fieldSetFlags()[$2L] ? "
                  + "($3T) this.$1L : "
                  + "($3T) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
              escapedFieldName, fieldIndex, fieldClass);
        }

      } else {
        fieldBuilder = FieldSpec.builder(fieldType, escapedFieldName, Modifier.PRIVATE);
        if(!AvroType.RECORD.equals(fieldAvroType)) {

          if (AvroType.ARRAY.equals(fieldSchema.type()) || SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.ARRAY, field.getSchema())) {
            buildMethodCodeBlockBuilder.addStatement(
                "record.$1L = fieldSetFlags()[$2L] ? "
                    + "com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8(this.$1L) : "
                    + "($3L) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8(com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L]))",
                escapedFieldName, fieldIndex, SpecificRecordGeneratorUtil.getTypeName(field.getSchema(), fieldAvroType, true,
                    config.getDefaultFieldStringRepresentation()));
          } else if (AvroType.MAP.equals(fieldSchema.type()) || SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.MAP, field.getSchema())) {
            buildMethodCodeBlockBuilder.addStatement(
                "record.$1L = fieldSetFlags()[$2L] ? "
                    + "com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8(this.$1L) : "
                    + "($3L) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8(com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L]))",
                escapedFieldName, fieldIndex, SpecificRecordGeneratorUtil.getTypeName(field.getSchema(), fieldAvroType, true,
                    config.getDefaultFieldStringRepresentation()));
          } else if (AvroType.UNION.equals(fieldSchema.type())) {
            buildMethodCodeBlockBuilder.beginControlFlow("if (fieldSetFlags()[$1L]  && $2L == null)", fieldIndex, escapedFieldName)
                .addStatement("record.$1L = null", escapedFieldName)
                .endControlFlow();
            // if union might contain string value in runtime
            for (SchemaOrRef unionMemberSchema : ((AvroUnionSchema) field.getSchema()).getTypes()) {
              if (unionMemberSchema.getSchema() != null && unionMemberSchema.getSchema().type().equals(AvroType.STRING)) {
                buildMethodCodeBlockBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, CharSequence.class)
                    .addStatement(
                        "record.$1L = fieldSetFlags()[$2L] ? "
                            + "com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8(this.$1L) : "
                            + "($3T) com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8(com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L]))",
                        escapedFieldName, fieldIndex,
                        SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(AvroType.STRING,
                            config.getDefaultFieldStringRepresentation(), true))
                    .endControlFlow();
              } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(unionMemberSchema.getSchema(), !config.isUtf8EncodingEnabled())) {
                buildMethodCodeBlockBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, List.class)
                    .addStatement(
                        "record.$1L = fieldSetFlags()[$2L] ? "
                            + "com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8(this.$1L) : "
                            + "($3L) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8(com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L]))",
                        escapedFieldName, fieldIndex, SpecificRecordGeneratorUtil.getTypeName(field.getSchema(), fieldAvroType, true,
                            config.getDefaultFieldStringRepresentation()))
                    .endControlFlow();
              } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(unionMemberSchema.getSchema(), !config.isUtf8EncodingEnabled())) {
                buildMethodCodeBlockBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, Map.class)
                    .addStatement(
                        "record.$1L = fieldSetFlags()[$2L] ? "
                            + "com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8(this.$1L) : "
                            + "($3L) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8(com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L]))",
                        escapedFieldName, fieldIndex, SpecificRecordGeneratorUtil.getTypeName(field.getSchema(), fieldAvroType, true,
                            config.getDefaultFieldStringRepresentation()))
                    .endControlFlow();
              }
            }
            buildMethodCodeBlockBuilder.beginControlFlow("else")
                .addStatement(
                    "record.$1L = fieldSetFlags()[$2L] ? ($3L) this.$1L : ($3L) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
                    escapedFieldName, fieldIndex, SpecificRecordGeneratorUtil.getTypeName(field.getSchema(), fieldAvroType, true,
                        config.getDefaultFieldStringRepresentation()))
                .endControlFlow();
          } else {
            buildMethodCodeBlockBuilder.addStatement(
                "record.$1L = fieldSetFlags()[$2L] ? ($3L) this.$1L : ($3L) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
                escapedFieldName, fieldIndex, SpecificRecordGeneratorUtil.getTypeName(field.getSchema(), fieldAvroType, true,
                    config.getDefaultFieldStringRepresentation()));
          }
        } else {
          buildMethodCodeBlockBuilder.addStatement(
              "record.$1L = fieldSetFlags()[$2L] ? this.$1L : ($3L) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
              escapedFieldName, fieldIndex, fieldType);
        }
      }
      if (field.hasDoc()) {
        fieldBuilder.addJavadoc(replaceSingleDollarSignWithDouble(field.getDoc()));
      }
      recordBuilder.addField(fieldBuilder.build());

      otherBuilderConstructorFromRecordBlockBuilder.beginControlFlow("if (isValidValue(fields()[$L], other.$L))", fieldIndex,
          escapedFieldName)
          .addStatement("this.$1L = deepCopyField(other.$1L, fields()[$2L].schema(), $3S)", escapedFieldName, fieldIndex,
              config.getDefaultMethodStringRepresentation().getJsonValue())
          .addStatement("fieldSetFlags()[$L] = true", fieldIndex)
          .endControlFlow();

      otherBuilderConstructorFromOtherBuilderBlockBuilder.beginControlFlow("if (isValidValue(fields()[$L], other.$L))", fieldIndex,
          escapedFieldName)
          .addStatement("this.$1L = deepCopyField(other.$1L, fields()[$2L].schema(), $3S)", escapedFieldName, fieldIndex,
              config.getDefaultMethodStringRepresentation().getJsonValue())
          .addStatement("fieldSetFlags()[$1L] = other.fieldSetFlags()[$1L]", fieldIndex)
          .endControlFlow();

      // get, set, has, clear methods
      populateAccessorMethodsBlock(accessorMethodSpecs, field, fieldClass, fieldType, recordSchema.getFullName(),
          fieldIndex, config);

      fieldIndex++;
    }


    // private constructor
    recordBuilder.addMethod(MethodSpec.constructorBuilder()
        .addStatement("super($L)", "SCHEMA$")
        .addModifiers(Modifier.PRIVATE)
        .addJavadoc("Creates a new Builder")
        .build());

    // private constructor from record
    recordBuilder.addMethod(MethodSpec.constructorBuilder()
        .addStatement("super($L)", "SCHEMA$")
        .addParameter(ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()), "other")
        .addModifiers(Modifier.PRIVATE)
        .addJavadoc("Creates a Builder by copying an existing Builder.\n")
        .addJavadoc("@param other The existing Builder to copy.")
        .addCode(otherBuilderConstructorFromRecordBlockBuilder.build())
        .build());

    // private constructor from other builder
    recordBuilder.addMethod(MethodSpec.constructorBuilder()
        .addStatement("super($L)", "other.schema()")
        .addParameter(ClassName.get(recordSchema.getFullName(), "Builder"), "other")
        .addModifiers(Modifier.PRIVATE)
        .addJavadoc("Creates a Builder by copying an existing Builder.\n")
        .addJavadoc("@param other The existing Builder to copy.")
        .addCode(otherBuilderConstructorFromOtherBuilderBlockBuilder.build())
        .build());


    // Accessor methods
    recordBuilder.addMethods(accessorMethodSpecs);

    //Build method
    //try
    buildMethodCodeBlockBuilder
        .addStatement("return record")
        .endControlFlow();

    buildMethodCodeBlockBuilder
        .beginControlFlow("catch ($T e)", Exception.class)
        .addStatement("throw new com.linkedin.avroutil1.compatibility.exception.AvroUtilException(e)")
        .endControlFlow();

    recordBuilder.addMethod(
        MethodSpec.methodBuilder("build")
            .addModifiers(Modifier.PUBLIC)
            .addException(Exception.class)
            .returns(ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()))
            .addCode(buildMethodCodeBlockBuilder.build())
            .build());
  }

  private List<MethodSpec> getNewBuilderMethods(AvroRecordSchema recordSchema) {
    MethodSpec noArgNewBuilderSpec = MethodSpec.methodBuilder("newBuilder")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(ClassName.get(recordSchema.getFullName(), "Builder"))
        .addJavadoc("Creates a new $1L RecordBuilder.\n@return A new $1L RecordBuilder\n", recordSchema.getSimpleName())
        .addStatement("return new $T.Builder()", ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()))
        .build();

    MethodSpec otherBuilderNewBuilderSpec = MethodSpec.methodBuilder("newBuilder")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addParameter(ClassName.get(recordSchema.getFullName(), "Builder"), "other")
        .returns(ClassName.get(recordSchema.getFullName(), "Builder"))
        .addJavadoc(
            "Creates a new $1L RecordBuilder by copying an existing Builder.\n"
                + "@param other The existing builder to copy.\n@return A new $1L RecordBuilder",
            recordSchema.getSimpleName())
        .beginControlFlow("if(other == null)")
        .addStatement("return new $T.Builder()", ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()))
        .endControlFlow()
        .beginControlFlow("else")
        .addStatement("return new $T.Builder(other)", ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()))
        .endControlFlow()
        .build();


    MethodSpec otherInstanceNewBuilderSpec = MethodSpec.methodBuilder("newBuilder")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addParameter(ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()), "other")
        .returns(ClassName.get(recordSchema.getFullName(), "Builder"))
        .addJavadoc(
            "Creates a new $1L RecordBuilder by copying an existing $1L instance.\n"
                + "@param other The existing instance to copy.\n@return A new $1L RecordBuilder",
            recordSchema.getSimpleName())
        .beginControlFlow("if(other == null)")
        .addStatement("return new $T.Builder()", ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()))
        .endControlFlow()
        .beginControlFlow("else")
        .addStatement("return new $T.Builder(other)", ClassName.get(recordSchema.getNamespace(), recordSchema.getSimpleName()))
        .endControlFlow()
        .build();

    return Arrays.asList(noArgNewBuilderSpec, otherBuilderNewBuilderSpec, otherInstanceNewBuilderSpec);
  }

  private void populateAccessorMethodsBlock(List<MethodSpec> accessorMethodSpecs, AvroSchemaField field,
      Class<?> fieldClass, TypeName fieldType, String parentClass, int fieldIndex, SpecificRecordGenerationConfig config) {
    String escapedFieldName = getFieldNameWithSuffix(field);
    //Getter
    MethodSpec.Builder getMethodBuilder =
        MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("get", escapedFieldName))
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Gets the value of the '$L' field.$L" + "@return The value.", field.getName(),
                getFieldJavaDoc(field));
    if(fieldClass != null) {
      getMethodBuilder.returns(fieldClass).addStatement("return ($T)$L", fieldClass, escapedFieldName);
    } else {
      getMethodBuilder.returns(fieldType).addStatement("return ($T)$L", fieldType, escapedFieldName);
    }

    //Setter
    MethodSpec.Builder setMethodBuilder =
        MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc(
                "Sets the value of the '$L' field.$L" + "@param value The value of '$L'.\n" + "@return This Builder.",
                field.getName(), getFieldJavaDoc(field), field.getName())
            .addStatement("validate(fields()[$L], value)", fieldIndex)
            .addStatement("this.$L = value", escapedFieldName)
            .addStatement("fieldSetFlags()[$L] = true", fieldIndex)
            .addStatement("return this")
            .returns(ClassName.get(parentClass, "Builder"));;
    if (fieldClass != null) {
      setMethodBuilder.addParameter(fieldClass, "value");
    } else {
      setMethodBuilder.addParameter(fieldType, "value");
    }

    // Additional setter for int/long fields (both primitive and boxed)
    MethodSpec.Builder overloadSetMethodBuilder = null;
    if ((SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.LONG, field.getSchema())
        || SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.INT, field.getSchema()))) {
      overloadSetMethodBuilder = createNumericTypeOverloadedSetterBuilder(fieldClass, field, escapedFieldName, fieldIndex,
          parentClass, config);
    }

    // Has
    MethodSpec.Builder hasMethodBuilder =
        MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("has", field.getName()))
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Checks whether the '$L' field has been set.$L"
                    + "@return True if the '$L' field has been set, false otherwise.", field.getName(),
                getFieldJavaDoc(field), field.getName())
            .addStatement("return fieldSetFlags()[$L]", fieldIndex)
            .returns(boolean.class);

    // Clear
    MethodSpec.Builder clearMethodBuilder =
        MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("clear", field.getName()))
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc(
                "Clears the value of the '$L' field.$L" + "@return This Builder.",
                field.getName(), getFieldJavaDoc(field));

    if (fieldClass == null) {
      clearMethodBuilder.addStatement("$L = null", escapedFieldName);
    }
    clearMethodBuilder.addStatement("fieldSetFlags()[$L] = false", fieldIndex)
        .addStatement("return this")
        .returns(ClassName.get(parentClass, "Builder"));

    accessorMethodSpecs.add(getMethodBuilder.build());
    accessorMethodSpecs.add(setMethodBuilder.build());
    if (overloadSetMethodBuilder != null) {
      accessorMethodSpecs.add(overloadSetMethodBuilder.build());
    }
    accessorMethodSpecs.add(hasMethodBuilder.build());
    accessorMethodSpecs.add(clearMethodBuilder.build());
  }

  /**
   * Creates an overloaded setter method for numeric type fields (int/long) to support flexible type conversion.
   *
   * @param fieldClass The class of the field (int.class, long.class, Integer.class, or Long.class)
   * @param field The Avro schema field
   * @param escapedFieldName The escaped field name
   * @param fieldIndex The index of the field
   * @param parentClass The parent class name
   * @return A MethodSpec.Builder for the overloaded setter method
   */
  private MethodSpec.Builder createNumericTypeOverloadedSetterBuilder(Class<?> fieldClass, AvroSchemaField field,
      String escapedFieldName, int fieldIndex, String parentClass, SpecificRecordGenerationConfig config) {
    MethodSpec.Builder methodBuilder = null;

    if (fieldClass != null) {
      methodBuilder = MethodSpec
          .methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
          .addModifiers(Modifier.PUBLIC);

      if (fieldClass.equals(long.class)) {
        // If field is of type long, add method with int parameter
        methodBuilder
            .addParameter(TypeName.INT, "value")
            .addStatement("validate(fields()[$L], value)", fieldIndex)
            .addStatement("this.$L = value", escapedFieldName);

      } else if (fieldClass.equals(int.class)) {
        // If field is of type int, add method with long parameter
        // This method will error if the long input is greater than Integer.MAX_VALUE
        methodBuilder
            .addParameter(TypeName.LONG, "value")
            .beginControlFlow("if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE)")
            .addStatement("this.$L = (int) value", escapedFieldName)
            .nextControlFlow("else")
            .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to int\")")
            .endControlFlow();

      }
    } else if (field.getSchema() != null && field.getSchema().type().equals(AvroType.UNION)) {
      TypeName typeName = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
          field.getSchemaOrRef().getSchema().type(), true,
          config.getDefaultMethodStringRepresentation());

      if (typeName.equals(ClassName.get(Long.class))) {
        // For long or Long fields, add a setter that accepts int or Integer
        methodBuilder = MethodSpec
            .methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
            .addParameter(ClassName.get(Integer.class), "value")
            .beginControlFlow("if (value == null)")
            .addStatement("this.$L = null", escapedFieldName)
            .nextControlFlow("else")
            .addStatement("this.$L = Long.valueOf(value)", escapedFieldName)
            .endControlFlow();


      } else if (typeName.equals(ClassName.get(Integer.class))) {
        methodBuilder = MethodSpec
            .methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
            .addParameter(ClassName.get(Long.class), "value")
            .beginControlFlow("if (value == null)")
            .addStatement("this.$L = null", escapedFieldName)
            .nextControlFlow("else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE)")
            .addStatement("this.$L = Integer.valueOf(value.intValue())", escapedFieldName)
            .nextControlFlow("else")
            .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to Integer\")")
            .endControlFlow();
      }
    }

    if (methodBuilder != null) {
      methodBuilder
          .addJavadoc(
              "Sets the value of the '$L' field.$L" + "@param value The value of '$L'.\n" + "@return This Builder.",
              field.getName(), getFieldJavaDoc(field), field.getName())
          .addStatement("fieldSetFlags()[$L] = true", fieldIndex)
          .addStatement("return this")
          .returns(ClassName.get(parentClass, "Builder"));
    }

    return methodBuilder;
  }

  private String getFieldJavaDoc(AvroSchemaField field) {
    return (field.hasDoc() ? "\n" + replaceSingleDollarSignWithDouble(field.getDoc()) + "\n" : "\n");
  }

  private String getSuffixForFieldName(String fieldName) {
    return (AvroRecordUtil.AVRO_RESERVED_FIELD_NAMES.contains(fieldName)) ? "$" : StringUtils.EMPTY_STRING;
  }

  private String getFieldNameWithSuffix(AvroSchemaField field) {
    return field.getName() + getSuffixForFieldName(field.getName());
  }

  private String getMethodNameForFieldWithPrefix(String prefix, String fieldName) {
    if (fieldName.isEmpty()) {
      throw new IllegalArgumentException("FieldName must be longer than 1");
    }

    // Converts a snake_case name to a PascalCase name
    String pascalCasedField = Arrays.stream(fieldName.split("_")).map(s -> {
      if (s.isEmpty()) {
        return s;
      } else {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
      }
    }).collect(Collectors.joining());

    return prefix + pascalCasedField + getSuffixForFieldName(fieldName);
  }

  private void addCustomDecodeMethod(MethodSpec.Builder customDecodeBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config, TypeSpec.Builder classBuilder, Counter sizeValCounter) {
    int blockSize = 25, fieldCounter = 0, chunkCounter = 0;
    // reset var counter
    sizeValCounter.reset();
    customDecodeBuilder.addStatement(
            "org.apache.avro.Schema.Field[] fieldOrder = (com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.areFieldsReordered(getSchema())) ? in.readFieldOrder() : null")
        .beginControlFlow("if (fieldOrder == null)");

    while (fieldCounter < recordSchema.getFields().size()) {
      String chunkMethodName = "customDecodeWithoutFieldOrderChunk" + chunkCounter;
      // add call to new method.
      customDecodeBuilder.addStatement(chunkMethodName + "(in)");
      // create new method
      MethodSpec.Builder customDecodeChunkMethod = MethodSpec.methodBuilder(chunkMethodName)
          .addParameter(SpecificRecordGeneratorUtil.CLASSNAME_RESOLVING_DECODER, "in")
          .addException(IOException.class)
          .addModifiers(Modifier.PUBLIC);
      for (; fieldCounter < Math.min(blockSize * chunkCounter + blockSize, recordSchema.getFields().size());
          fieldCounter++) {
        AvroSchemaField field = recordSchema.getField(fieldCounter);
        boolean unionContainsBothIntAndLong = field.getSchemaOrRef().getSchema().type().equals(AvroType.UNION) &&
            unionSchemaContainsBothIntAndLong((AvroUnionSchema) field.getSchemaOrRef().getSchema());

        String escapedFieldName = getFieldNameWithSuffix(field);
        customDecodeChunkMethod.addStatement(getSerializedCustomDecodeBlock(config, field.getSchemaOrRef().getSchema(),
            field.getSchemaOrRef().getSchema().type(), "this." + replaceSingleDollarSignWithDouble(escapedFieldName),
            "this." + replaceSingleDollarSignWithDouble(escapedFieldName), StringUtils.EMPTY_STRING,
            sizeValCounter, unionContainsBothIntAndLong));
      }
      chunkCounter++;
      classBuilder.addMethod(customDecodeChunkMethod.build());
    }

    // reset var counter
    sizeValCounter.reset();
    int fieldIndex = 0;
    fieldCounter = 0;
    chunkCounter = 0;
    customDecodeBuilder.endControlFlow()
        .beginControlFlow("else");

    while (fieldCounter < recordSchema.getFields().size()) {
      String chunkMethodName = "customDecodeWithFieldOrderChunk" + chunkCounter;
      // add call to new method.
      customDecodeBuilder.addStatement(chunkMethodName + "(in, fieldOrder)");
      // create new method
      MethodSpec.Builder customDecodeChunkMethod = MethodSpec.methodBuilder(chunkMethodName)
          .addParameter(SpecificRecordGeneratorUtil.CLASSNAME_RESOLVING_DECODER, "in")
          .addParameter(ArrayTypeName.of(SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA_FIELD), "fieldOrder")
          .addException(IOException.class)
          .addModifiers(Modifier.PUBLIC);

      customDecodeChunkMethod.beginControlFlow("for( int i = $L; i< $L; i++)", fieldCounter, Math.min(blockSize * chunkCounter + blockSize, recordSchema.getFields().size()))
          .beginControlFlow("switch(fieldOrder[i].pos())");

      for (; fieldCounter < Math.min(blockSize * chunkCounter + blockSize, recordSchema.getFields().size());
          fieldCounter++) {
        AvroSchemaField field = recordSchema.getField(fieldCounter);
        String escapedFieldName = getFieldNameWithSuffix(field);
        boolean unionContainsBothIntAndLong = field.getSchemaOrRef().getSchema().type().equals(AvroType.UNION) &&
            unionSchemaContainsBothIntAndLong((AvroUnionSchema) field.getSchemaOrRef().getSchema());
        customDecodeChunkMethod.addStatement(String.format("case %s: ",fieldIndex++)+ getSerializedCustomDecodeBlock(config,
                field.getSchemaOrRef().getSchema(), field.getSchemaOrRef().getSchema().type(),
                "this." + replaceSingleDollarSignWithDouble(escapedFieldName),
                "this." + replaceSingleDollarSignWithDouble(escapedFieldName), StringUtils.EMPTY_STRING,
                sizeValCounter, unionContainsBothIntAndLong))
            .addStatement("break");
      }
      customDecodeChunkMethod
          //switch
          .endControlFlow()
          //for
          .endControlFlow();

      chunkCounter++;
      classBuilder.addMethod(customDecodeChunkMethod.build());
    }
        //else
    customDecodeBuilder.endControlFlow();

  }


  private String getSerializedCustomDecodeBlock(SpecificRecordGenerationConfig config,
      AvroSchema fieldSchema, AvroType fieldType, String fieldName, String schemaFieldName, String arrayOption,
      Counter sizeValCounter, boolean unionContainsBothIntAndLong) {
    String serializedCodeBlock = "";
    CodeBlock.Builder codeBlockBuilder  = CodeBlock.builder();
    switch (fieldType) {

      case NULL:
        serializedCodeBlock = "in.readNull()";
        break;
      case BOOLEAN:
        serializedCodeBlock = String.format("%s = in.readBoolean()", fieldName);
        break;
      case INT:
        if (unionContainsBothIntAndLong) {
          serializedCodeBlock = String.format("%s = in.readInt()", fieldName);
        } else {
          String cleanIntFieldName = fieldName.replaceAll("^this\\.", "");
          String tempIntVarName = "temp" + Character.toUpperCase(cleanIntFieldName.charAt(0)) + cleanIntFieldName.substring(1);
          codeBlockBuilder
              .beginControlFlow("try")
              .addStatement("$L = in.readInt()", fieldName)
              .nextControlFlow("catch (Exception e)")
              .addStatement("// If int decoding fails, try long decoding with bounds check")
              .addStatement("long $L = in.readLong()", tempIntVarName)
              .beginControlFlow("if ($L <= Integer.MAX_VALUE && $L >= Integer.MIN_VALUE)", tempIntVarName, tempIntVarName)
              .addStatement("$L = (int) $L", fieldName, tempIntVarName)
              .nextControlFlow("else")
              .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value cannot be cast to int\")")
              .endControlFlow()
              .endControlFlow();
          serializedCodeBlock = codeBlockBuilder.build().toString();
        }
        break;
      case FLOAT:
        serializedCodeBlock = String.format("%s = in.readFloat()", fieldName);
        break;
      case LONG:
        if (unionContainsBothIntAndLong) {
          serializedCodeBlock = String.format("%s = in.readLong()", fieldName);
        } else {
          String cleanLongFieldName = fieldName.replaceAll("^this\\.", "");
          String tempLongVarName =
              "temp" + Character.toUpperCase(cleanLongFieldName.charAt(0)) + cleanLongFieldName.substring(1);
          codeBlockBuilder.beginControlFlow("try")
              .addStatement("$L = in.readLong()", fieldName)
              .nextControlFlow("catch (Exception e)")
              .addStatement("// If long decoding fails, try int decoding with conversion to long")
              .addStatement("int $L = in.readInt()", tempLongVarName)
              .addStatement("$L = (long) $L", fieldName, tempLongVarName)
              .endControlFlow();
          serializedCodeBlock = codeBlockBuilder.build().toString();
        }
        break;
      case DOUBLE:
        serializedCodeBlock = String.format("%s = in.readDouble()", fieldName);
        break;
      case BYTES:
        serializedCodeBlock = String.format("%s = in.readBytes((java.nio.ByteBuffer) %s)", fieldName, fieldName);
        break;
      case STRING:
        serializedCodeBlock =
            String.format("%s = in.readString(%s instanceof org.apache.avro.util.Utf8 ? (org.apache.avro.util.Utf8)%s : null)", fieldName, fieldName, fieldName);
        break;
      case ENUM:
        TypeName enumClassName = SpecificRecordGeneratorUtil.getTypeName(fieldSchema, AvroType.ENUM, true, config.getDefaultFieldStringRepresentation());
        serializedCodeBlock =
            String.format("%s = com.linkedin.avroutil1.Enums.getConstant(%s.class, in.readEnum())", fieldName, enumClassName.toString());
        break;
      case FIXED:
        codeBlockBuilder.beginControlFlow("if ($L == null)", fieldName)
            .addStatement("$L = new $L()", fieldName,
                SpecificRecordGeneratorUtil.getTypeName(fieldSchema, AvroType.FIXED, true,
                    config.getDefaultMethodStringRepresentation()).toString())
            .endControlFlow()
            .addStatement("in.readFixed((($T)$L).bytes(), 0, $L)",
                SpecificRecordGeneratorUtil.getTypeName(fieldSchema, AvroType.FIXED, false,
                    config.getDefaultMethodStringRepresentation()), fieldName,
            ((AvroFixedSchema) fieldSchema).getSize());

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
      case ARRAY:
        sizeValCounter.increment();

        String arrayVarName = getArrayVarName(sizeValCounter.get());
        String gArrayVarName = getGArrayVarName(sizeValCounter.get());
        String arraySizeVarName = getSizeVarName(sizeValCounter.get());
        String arrayElementVarName = getElementVarName(sizeValCounter.get());
        AvroSchema arrayItemSchema = ((AvroArraySchema) fieldSchema).getValueSchema();
        Class<?> arrayItemClass =
            SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(arrayItemSchema.type(),
                config.getDefaultFieldStringRepresentation(), true);
        TypeName arrayItemTypeName =
            SpecificRecordGeneratorUtil.getTypeName(arrayItemSchema, arrayItemSchema.type(), true,
                config.getDefaultFieldStringRepresentation());

        codeBlockBuilder
            .addStatement("long $L = in.readArrayStart()", arraySizeVarName)
            .addStatement("$1T<$2T> $3L = ($1T<$2T>)$4L", List.class,
                arrayItemClass != null ? arrayItemClass : arrayItemTypeName, arrayVarName, fieldName)
            .beginControlFlow("if($L == null)", arrayVarName)
            .addStatement(
                "$L = new org.apache.avro.specific.SpecificData.Array<$T>((int)$L, $L.getField($S).schema()$L)",
                arrayVarName, arrayItemClass != null ? arrayItemClass : arrayItemTypeName, arraySizeVarName, "SCHEMA$$",
                SpecificRecordGeneratorUtil.removePrefixFromFieldName(schemaFieldName), arrayOption)
            .endControlFlow()
            .beginControlFlow("else")
            .addStatement("$L.clear()", arrayVarName)
            .endControlFlow();

        codeBlockBuilder.addStatement(
            "org.apache.avro.specific.SpecificData.Array<$T> $L = ($L instanceof org.apache.avro.specific.SpecificData.Array ? (org.apache.avro.specific.SpecificData.Array<$T>)$L : null)",
            arrayItemClass != null ? arrayItemClass : arrayItemTypeName, gArrayVarName, arrayVarName, arrayItemClass != null ? arrayItemClass : arrayItemTypeName, arrayVarName);
        codeBlockBuilder.beginControlFlow("for (; 0 < $1L; $1L = in.arrayNext())", arraySizeVarName)
            .beginControlFlow("for(; $1L != 0; $1L--)", arraySizeVarName)
            .addStatement("$T $L = ($L != null ? $L.peek() : null)", arrayItemClass != null ? arrayItemClass : arrayItemTypeName, arrayElementVarName, gArrayVarName, gArrayVarName);

        codeBlockBuilder.addStatement(
            getSerializedCustomDecodeBlock(config, arrayItemSchema, arrayItemSchema.type(), arrayElementVarName,
                schemaFieldName,  arrayOption + SpecificRecordGeneratorUtil.ARRAY_GET_ELEMENT_TYPE,
                sizeValCounter, unionContainsBothIntAndLong));
        codeBlockBuilder.addStatement("$L.add($L)", arrayVarName, arrayElementVarName)
            .endControlFlow()
            .endControlFlow()
            .addStatement("$L = com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8($L)", fieldName, arrayVarName);

        serializedCodeBlock = codeBlockBuilder.build().toString();


        break;
      case MAP:
        sizeValCounter.increment();
        String mapVarName = getMapVarName(sizeValCounter.get());
        String mapKeyVarName = getKeyVarName(sizeValCounter.get());
        String mapSizeVarName = getSizeVarName(sizeValCounter.get());
        String mapValueVarName = getValueVarName(sizeValCounter.get());
        AvroType mapItemAvroType = ((AvroMapSchema) fieldSchema).getValueSchema().type();
        Class<?> mapItemClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(mapItemAvroType,
            config.getDefaultFieldStringRepresentation(), true);
        TypeName mapItemClassName =
            SpecificRecordGeneratorUtil.getTypeName(((AvroMapSchema) fieldSchema).getValueSchema(), mapItemAvroType,
                true, config.getDefaultFieldStringRepresentation());

        codeBlockBuilder
            .addStatement("long $L = in.readMapStart()", mapSizeVarName);

        codeBlockBuilder.addStatement("$1T<$2T,$3T> $4L = ($1T<$2T,$3T>)$5L", Map.class, CharSequence.class,
            ((mapItemClass != null) ? mapItemClass : mapItemClassName), mapVarName, fieldName);

        codeBlockBuilder.beginControlFlow("if($L == null)", mapVarName)
          .addStatement("$L = new $T<$T,$T>((int)$L)", mapVarName, HashMap.class, CharSequence.class,
              ((mapItemClass != null) ? mapItemClass : mapItemClassName), mapSizeVarName)
          .endControlFlow()
          .beginControlFlow("else")
          .addStatement("$L.clear()", mapVarName)
          .endControlFlow();

        codeBlockBuilder.beginControlFlow("for (; 0 < $1L; $1L = in.mapNext())", mapSizeVarName)
            .beginControlFlow("for(; $1L != 0; $1L--)", mapSizeVarName)
            .addStatement("$T $L = null", CharSequence.class, mapKeyVarName)
            .addStatement(
                getSerializedCustomDecodeBlock(config, ((AvroMapSchema) fieldSchema).getValueSchema(), AvroType.STRING,
                    mapKeyVarName, schemaFieldName, arrayOption + SpecificRecordGeneratorUtil.MAP_GET_VALUE_TYPE,
                    sizeValCounter, unionContainsBothIntAndLong))
            .addStatement("$T $L = null", ((mapItemClass != null) ? mapItemClass : mapItemClassName), mapValueVarName)
            .addStatement(getSerializedCustomDecodeBlock(config, ((AvroMapSchema) fieldSchema).getValueSchema(),
                ((AvroMapSchema) fieldSchema).getValueSchema().type(), mapValueVarName, schemaFieldName,
                arrayOption + SpecificRecordGeneratorUtil.MAP_GET_VALUE_TYPE,
                sizeValCounter, unionContainsBothIntAndLong));

        codeBlockBuilder.addStatement("$L.put($L,$L)", mapVarName, mapKeyVarName, mapValueVarName)
            .endControlFlow()
            .endControlFlow()
            .addStatement("$L = com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8($L)", fieldName, mapVarName);;

        serializedCodeBlock = codeBlockBuilder.build().toString();

        break;

      case UNION:
        int numberOfUnionMembers = ((AvroUnionSchema) fieldSchema).getTypes().size();
        if (numberOfUnionMembers > 0) {
          codeBlockBuilder.beginControlFlow("switch(in.readIndex())");
          for (int i = 0; i < numberOfUnionMembers; i++) {

            SchemaOrRef unionMember = ((AvroUnionSchema) fieldSchema).getTypes().get(i);
            codeBlockBuilder.addStatement("case $L: ", i);
            codeBlockBuilder.addStatement(
                getSerializedCustomDecodeBlock(config, unionMember.getSchema(), unionMember.getSchema().type(),
                    fieldName, schemaFieldName, arrayOption + ".getTypes().get(" + i + ")",
                    sizeValCounter, unionContainsBothIntAndLong));
            if (unionMember.getSchema().type().equals(AvroType.NULL)) {
              codeBlockBuilder.addStatement("$L = null", fieldName);
            }
            codeBlockBuilder.addStatement("break");
          }
          codeBlockBuilder.addStatement("default:")
              .addStatement("throw new $T($S)", IndexOutOfBoundsException.class, "Union IndexOutOfBounds")
              .endControlFlow();

          serializedCodeBlock = codeBlockBuilder.build().toString();
        } else {
          LOGGER.warn("Unions with Zero types are not recommended and poorly supported. "
              + "Please consider using a nullable field instead. Field name: " + fieldName);
        }
        break;
      case RECORD:
        TypeName className = SpecificRecordGeneratorUtil.getTypeName(fieldSchema, fieldType, true,
            config.getDefaultFieldStringRepresentation());

        codeBlockBuilder.beginControlFlow("if($L == null)", fieldName)
        .addStatement("$L = new $T()", fieldName, className)
        .endControlFlow()
        .addStatement("(($T)$L).customDecode(in)", className, fieldName);

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
    }
    return SpecificRecordGeneratorUtil.SINGLE_DOLLAR_SIGN_REGEX.matcher(serializedCodeBlock).replaceAll("\\$\\$");
  }

  private void addCustomEncodeMethod(MethodSpec.Builder customEncodeBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config, Counter sizeValCounter) {
    for(AvroSchemaField field : recordSchema.getFields()) {
      String escapedFieldName = getFieldNameWithSuffix(field);
      customEncodeBuilder.addStatement(getSerializedCustomEncodeBlock(config, field.getSchemaOrRef().getSchema(),
          field.getSchemaOrRef().getSchema().type(), "this."+replaceSingleDollarSignWithDouble(escapedFieldName),
          sizeValCounter));
    }
  }

  private String getSerializedCustomEncodeBlock(SpecificRecordGenerationConfig config,
      AvroSchema fieldSchema, AvroType fieldType, String fieldName, Counter sizeValCounter) {
    String serializedCodeBlock = "";
    CodeBlock.Builder codeBlockBuilder  = CodeBlock.builder();
    switch (fieldType) {

      case NULL:
        serializedCodeBlock = "out.writeNull()";
        break;
      case BOOLEAN:
        serializedCodeBlock = String.format("out.writeBoolean((Boolean) %s)", fieldName);
        break;
      case INT:
        serializedCodeBlock = String.format("out.writeInt((Integer) %s)", fieldName);
        break;
      case LONG:
        serializedCodeBlock = String.format("out.writeLong((Long) %s)", fieldName);
        break;
      case FLOAT:
        serializedCodeBlock = String.format("out.writeFloat((Float) %s)", fieldName);
        break;
      case STRING:
        serializedCodeBlock = CodeBlock.builder()
            .addStatement("out.writeString(($T)$L)",
                SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(fieldType,
                    config.getDefaultFieldStringRepresentation(), false), fieldName)
            .build()
            .toString();
        break;
      case DOUBLE:
        serializedCodeBlock = String.format("out.writeDouble((Double) %s)", fieldName);
        break;
      case BYTES:
        serializedCodeBlock = String.format("out.writeBytes((java.nio.ByteBuffer) %s)", fieldName);
        break;
      case ENUM:
        serializedCodeBlock = CodeBlock.builder()
            .addStatement("out.writeEnum((($T)$L).ordinal())",
                SpecificRecordGeneratorUtil.getTypeName(fieldSchema, AvroType.ENUM, false,
                    config.getDefaultMethodStringRepresentation()), fieldName)
            .build()
            .toString();
        break;
      case FIXED:
        serializedCodeBlock = CodeBlock.builder()
            .addStatement("out.writeFixed((($T)$L).bytes(), 0, $L)",
                SpecificRecordGeneratorUtil.getTypeName(fieldSchema, AvroType.FIXED, false,
                    config.getDefaultMethodStringRepresentation()), fieldName,
                ((AvroFixedSchema) fieldSchema).getSize())
            .build()
            .toString();
        break;
      case ARRAY:
        sizeValCounter.increment();
        String lengthVarName = getSizeVarName(sizeValCounter.get());
        String actualSizeVarName = getActualSizeVarName(sizeValCounter.get());
        AvroType arrayItemAvroType = ((AvroArraySchema) fieldSchema).getValueSchema().type();
        Class<?> arrayItemClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(arrayItemAvroType,
            config.getDefaultFieldStringRepresentation(), true);
        TypeName arrayItemTypeName =
            SpecificRecordGeneratorUtil.getTypeName(((AvroArraySchema) fieldSchema).getValueSchema(), arrayItemAvroType,
                true, config.getDefaultFieldStringRepresentation());
        codeBlockBuilder.addStatement("long $L = ((java.util.List)$L).size()", lengthVarName, fieldName)
            .addStatement("out.writeArrayStart()")
            .addStatement("out.setItemCount($L)", lengthVarName)
            .addStatement("long $L = 0", actualSizeVarName)
            .beginControlFlow("for ($1T $2L: (java.util.List<$1T>)$3L)", arrayItemClass != null ? arrayItemClass : arrayItemTypeName,
                getElementVarName(sizeValCounter.get()), fieldName)
            .addStatement("$L++", actualSizeVarName)
            .addStatement("out.startItem()")
            .addStatement(getSerializedCustomEncodeBlock(config, ((AvroArraySchema) fieldSchema).getValueSchema(),
                arrayItemAvroType, getElementVarName(sizeValCounter.get()), sizeValCounter))
            .endControlFlow();

        codeBlockBuilder
            .addStatement("out.writeArrayEnd()")
            .beginControlFlow("if ($L != $L)", actualSizeVarName, lengthVarName)
            .addStatement("throw new $T(\"Array-size written was \" + $L + \", but element count was \" + $L + \".\")",
            ConcurrentModificationException.class, lengthVarName, actualSizeVarName);
        codeBlockBuilder.endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
      case MAP:
        sizeValCounter.increment();
        lengthVarName = getSizeVarName(sizeValCounter.get());
        actualSizeVarName = getActualSizeVarName(sizeValCounter.get());
        String elementVarName = getElementVarName(sizeValCounter.get());
        String valueVarName = getValueVarName(sizeValCounter.get());

        codeBlockBuilder
            .addStatement("long $L = ((Map)$L).size()", lengthVarName, fieldName)
            .addStatement("out.writeMapStart()")
            .addStatement(" out.setItemCount($L)", lengthVarName)
            .addStatement("long $L = 0", actualSizeVarName);

        AvroType mapItemAvroType = ((AvroMapSchema) fieldSchema).getValueSchema().type();
        Class<?> mapItemClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(mapItemAvroType,
            config.getDefaultFieldStringRepresentation(), true);
        TypeName mapItemClassName =
            SpecificRecordGeneratorUtil.getTypeName(((AvroMapSchema) fieldSchema).getValueSchema(), mapItemAvroType,
                true, config.getDefaultFieldStringRepresentation());

        codeBlockBuilder.beginControlFlow("for (java.util.Map.Entry<java.lang.CharSequence, $1T> $2L: ((java.util.Map<java.lang.CharSequence, $1T>)$3L).entrySet())",
            (mapItemClass != null) ? mapItemClass : mapItemClassName, elementVarName, fieldName);

        codeBlockBuilder
            .addStatement("$L++", actualSizeVarName)
            .addStatement("out.startItem()")
            .addStatement("out.writeString($L.getKey())", elementVarName);

        codeBlockBuilder.addStatement("$T $L = $L.getValue()", (mapItemClass != null) ? mapItemClass : mapItemClassName,
            valueVarName, elementVarName);

        codeBlockBuilder.addStatement(
            getSerializedCustomEncodeBlock(config, ((AvroMapSchema) fieldSchema).getValueSchema(), mapItemAvroType,
                valueVarName, sizeValCounter))
            .endControlFlow()
            .addStatement("out.writeMapEnd()")
            .beginControlFlow("if ($L != $L)", actualSizeVarName, lengthVarName)
            .addStatement("throw new $T(\"Map-size written was \" + $L + \", but element count was \" + $L + \".\")",
            ConcurrentModificationException.class, lengthVarName, actualSizeVarName)
            .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
      case UNION:
        int numberOfUnionMembers = ((AvroUnionSchema) fieldSchema).getTypes().size();
        if (numberOfUnionMembers > 0) {
          for (int i = 0; i < numberOfUnionMembers; i++) {
            AvroSchema unionMemberSchema = ((AvroUnionSchema) fieldSchema).getTypes().get(i).getSchema();
            Class<?> unionMemberType =
                SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(unionMemberSchema.type(),
                    config.getDefaultFieldStringRepresentation(), true);
            TypeName unionMemberTypeName =
                SpecificRecordGeneratorUtil.getTypeName(unionMemberSchema, unionMemberSchema.type(), false,
                    config.getDefaultFieldStringRepresentation());

            if (i == 0) {
              if (unionMemberSchema.type().equals(AvroType.NULL)) {
                codeBlockBuilder.beginControlFlow("if ($L == null) ", fieldName);
              } else {
                codeBlockBuilder.beginControlFlow("if ($L instanceof $T) ", fieldName,
                    unionMemberType != null ? unionMemberType : unionMemberTypeName);
              }
            } else {
              codeBlockBuilder.endControlFlow();
              if (unionMemberSchema.type().equals(AvroType.NULL)) {
                codeBlockBuilder.beginControlFlow(" else if ($L == null) ", fieldName);
              } else {
                codeBlockBuilder.beginControlFlow(" else if ($L instanceof $T) ", fieldName,
                    unionMemberType != null ? unionMemberType : unionMemberTypeName);
              }
            }
            codeBlockBuilder.addStatement("out.writeIndex($L)", i)
                .addStatement(
                    getSerializedCustomEncodeBlock(config, unionMemberSchema, unionMemberSchema.type(), fieldName,
                        sizeValCounter));
          }
          codeBlockBuilder.endControlFlow()
              .beginControlFlow("else")
              .addStatement("throw new $T($S)", IllegalArgumentException.class, "Value does not match any union member")
              .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        }
        break;
      case RECORD:
        TypeName className = SpecificRecordGeneratorUtil.getTypeName(fieldSchema, fieldType, true,
            config.getDefaultFieldStringRepresentation());
        serializedCodeBlock =
            CodeBlock.builder().addStatement("(($T)$L).customEncode(out)", className, fieldName).build().toString();
        break;
    }
    return SpecificRecordGeneratorUtil.SINGLE_DOLLAR_SIGN_REGEX.matcher(serializedCodeBlock).replaceAll("\\$\\$");
  }

  private String getKeyVarName(int sizeValCounter) {
    return "k" + sizeValCounter;
  }

  private String getValueVarName(int sizeValCounter) {
    return "v" + sizeValCounter;
  }

  private String getElementVarName(int sizeValCounter) {
    return "e" + sizeValCounter;
  }

  private String getSizeVarName(int sizeValCounter) {
    return "size" + sizeValCounter;
  }

  private String getActualSizeVarName(int sizeValCounter) {
    return "actualSize" + sizeValCounter;
  }

  private String getArrayVarName(int sizeValCounter) {
    return "a" + sizeValCounter;
  }

  private String getGArrayVarName(int sizeValCounter) {
    return "ga" + sizeValCounter;
  }

  private String getMapVarName(int sizeValCounter) {
    return "m" + sizeValCounter;
  }


  private void addPutByIndexMethod(TypeSpec.Builder classBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) {
    int fieldIndex = 0;
    MethodSpec.Builder methodSpecBuilder = MethodSpec.methodBuilder("put")
        .addParameter(int.class, "field")
        .addAnnotation(Override.class)
        .addParameter(Object.class, "value")
        .addModifiers(Modifier.PUBLIC);
    CodeBlock.Builder switchBuilder = CodeBlock.builder();
    switchBuilder.beginControlFlow("switch (field)");
    for (AvroSchemaField field : recordSchema.getFields()) {
      String escapedFieldName = getFieldNameWithSuffix(field);
      Class<?> fieldClass =
          SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(field.getSchemaOrRef().getSchema().type(),
              config.getDefaultFieldStringRepresentation(), false);

      if (config.isUtf8EncodingInPutByIndexEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, field.getSchema())) {
        // Default during transition, stores Utf8 in runtime for string fields
        if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
          switchBuilder.addStatement(
              "case $L: this.$L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getString(value); break",
              fieldIndex++, escapedFieldName);
        } else {
          switchBuilder.addStatement(
              "case $L: this.$L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8(value); break",
              fieldIndex++, escapedFieldName);
        }
      } else if (config.isUtf8EncodingInPutByIndexEnabled() && SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(field.getSchema(),!config.isUtf8EncodingEnabled())) {
        if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
          switchBuilder.addStatement(
              "case $1L: this.$2L = ($3T) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToString(value); break",
              fieldIndex++, escapedFieldName,
              SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                  field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
        } else {
          switchBuilder.addStatement(
              "case $1L: this.$2L = ($3T) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8(value); break",
              fieldIndex++, escapedFieldName,
              SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                  field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
        }
      } else if (config.isUtf8EncodingInPutByIndexEnabled() && SpecificRecordGeneratorUtil.isMapTransformerApplicable(field.getSchema(),!config.isUtf8EncodingEnabled())) {
        if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
          switchBuilder.addStatement(
              "case $1L: this.$2L = ($3T) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToString(value); break",
              fieldIndex++, escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                  field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
        } else {
          switchBuilder.addStatement(
              "case $1L: this.$2L = ($3T) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8(value); break",
              fieldIndex++, escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                  field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
        }
      } else if (field.getSchema() != null && AvroType.UNION.equals(field.getSchema().type())) {
        switchBuilder.add("case $L: \n", fieldIndex++);
        switchBuilder.beginControlFlow("if (value == null)")
            .addStatement("this.$1L = null", escapedFieldName)
            .endControlFlow();

        boolean unionContainsBothIntAndLong = unionSchemaContainsBothIntAndLong((AvroUnionSchema) field.getSchema());

        // if union might contain string value in runtime
        for (SchemaOrRef unionMemberSchema : ((AvroUnionSchema) field.getSchema()).getTypes()) {
          if (config.isUtf8EncodingInPutByIndexEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, unionMemberSchema.getSchema())) {
            switchBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, CharSequence.class);
            if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
              switchBuilder.addStatement(
                  "this.$L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getString(value); break",
                  escapedFieldName);
            } else {
              switchBuilder.addStatement(
                  "this.$L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8(value); break",
                  escapedFieldName);
            }
            switchBuilder.endControlFlow();

          } else if (config.isUtf8EncodingInPutByIndexEnabled() && SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(unionMemberSchema.getSchema(),!config.isUtf8EncodingEnabled())) {
            switchBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, List.class);
            if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
              switchBuilder.addStatement(
                  "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToString(value); break",
                  escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                      field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
            } else {
              switchBuilder.addStatement(
                  "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8(value); break",
                  escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                      field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
            }
            switchBuilder.endControlFlow();

          } else if (config.isUtf8EncodingInPutByIndexEnabled() && SpecificRecordGeneratorUtil.isMapTransformerApplicable(unionMemberSchema.getSchema(),!config.isUtf8EncodingEnabled())) {
            switchBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, Map.class);
            if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
              switchBuilder.addStatement(
                  "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToString(value); break",
                  escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                      field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
            } else {
              switchBuilder.addStatement(
                  "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8(value); break",
                  escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                      field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
            }
            switchBuilder.endControlFlow();
          } else if (!unionContainsBothIntAndLong && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.INT, unionMemberSchema.getSchema())) {
            // For boxed Integer fields, handle Long values with bounds check
            switchBuilder
                .beginControlFlow("else if (value instanceof Long)")
                .beginControlFlow("if ((Long) value <= Integer.MAX_VALUE && (Long) value >= Integer.MIN_VALUE)")
                .addStatement("this.$1L = ((Long) value).intValue()", escapedFieldName)
                .nextControlFlow("else")
                .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to Integer\")")
                .endControlFlow()
                .endControlFlow();
          } else if (!unionContainsBothIntAndLong && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.LONG, unionMemberSchema.getSchema())) {
            // For boxed Long fields, handle Integer values
            switchBuilder.beginControlFlow("else if (value instanceof Integer)")
                .addStatement("this.$1L = ((Integer) value).longValue()", escapedFieldName)
                .addStatement("break")
                .endControlFlow();
          }
        }
        switchBuilder.beginControlFlow("else")
            .addStatement("this.$L = ($L) value", escapedFieldName,
                SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                    field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()))
            .endControlFlow()
            .addStatement("break");
      } else if (fieldClass == int.class) {
        // If the field is an int, allow input value to be a long with range check
        switchBuilder
            .add("case $L: \n", fieldIndex++)
            .beginControlFlow("if (value instanceof Long)")
            .beginControlFlow("if (((Long) value) <= Integer.MAX_VALUE && ((Long) value) >= Integer.MIN_VALUE)")
            .addStatement("this.$1L = ((Long) value).intValue()", escapedFieldName)
            .endControlFlow()
            .beginControlFlow("else")
            .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to int\")")
            .endControlFlow()
            .endControlFlow()
            .beginControlFlow("else")
            .addStatement("this.$L = ($T) value",  escapedFieldName, fieldClass)
            .endControlFlow()
            .addStatement("break");
      } else if (fieldClass == long.class) {
        // If the field is a long, allow input value to be an int
        switchBuilder
            .add("case $L: \n", fieldIndex++)
            .beginControlFlow("if (value instanceof Integer)")
            .addStatement("this.$1L = ((Integer) value).longValue()", escapedFieldName)
            .endControlFlow()
            .beginControlFlow("else")
            .addStatement("this.$L = ($T) value",  escapedFieldName, fieldClass)
            .endControlFlow()
            .addStatement("break");
      } else {
        switchBuilder.addStatement(
            fieldClass != null ? "case $L: this.$L = ($T) value; break" : "case $L: this.$L = ($L) value; break",
            fieldIndex++, escapedFieldName, fieldClass != null ? fieldClass
                : SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                    field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
      }
    }
    switchBuilder.addStatement("default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\")")
        .endControlFlow();

    classBuilder.addMethod(methodSpecBuilder.addCode(switchBuilder.build()).build());
  }

  private void addGetByIndexMethod(TypeSpec.Builder classBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) {
    int fieldIndex = 0;
    MethodSpec.Builder methodSpecBuilder = MethodSpec.methodBuilder("get")
        .returns(Object.class)
        .addParameter(int.class, "field")
        .addAnnotation(Override.class)
        .addModifiers(Modifier.PUBLIC);
    CodeBlock.Builder switchBuilder = CodeBlock.builder();
    switchBuilder.beginControlFlow("switch (field)");
    for (AvroSchemaField field : recordSchema.getFields()) {
      String escapedFieldName = getFieldNameWithSuffix(field);
      if (config.isUtf8EncodingEnabled() &&  SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, field.getSchema())) {
        Class<?> fieldClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(AvroType.STRING,
            config.getDefaultMethodStringRepresentation(), false);
        switchBuilder.addStatement("case $L: return com.linkedin.avroutil1.compatibility.StringConverterUtil.get$L(this.$L)", fieldIndex++, fieldClass.getSimpleName(), escapedFieldName);
      } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(field.getSchema(), !config.isUtf8EncodingEnabled())) {
        switchBuilder.addStatement(
            "case $L: return com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.get$LList(this.$L, $L)",
            fieldIndex++, config.getDefaultMethodStringRepresentation().getJsonValue(), escapedFieldName,
            SpecificRecordGeneratorUtil.isCollectionSchemaValuePrimitive(field.getSchema(), !config.isUtf8EncodingEnabled()));
      } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(field.getSchema(), !config.isUtf8EncodingEnabled())) {
        switchBuilder.addStatement(
            "case $L: return com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.get$LMap(this.$L)",
            fieldIndex++, config.getDefaultMethodStringRepresentation().getJsonValue(), escapedFieldName);
      } else if (field.getSchema() != null && AvroType.UNION.equals(field.getSchema().type())) {

        switchBuilder.addStatement("case $L:", fieldIndex++);
        switchBuilder.beginControlFlow("if (this.$1L == null)", escapedFieldName)
            .addStatement("return null", escapedFieldName)
            .endControlFlow();

        // if union might contain string value in runtime
        for (SchemaOrRef unionMemberSchema : ((AvroUnionSchema) field.getSchema()).getTypes()) {
          if (config.isUtf8EncodingEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, unionMemberSchema.getSchema())) {
            switchBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, CharSequence.class)
                .addStatement("return com.linkedin.avroutil1.compatibility.StringConverterUtil.get$1L($2L)",
                    config.getDefaultMethodStringRepresentation().getJsonValue(),
                    escapedFieldName)
                .endControlFlow();
          } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(
              unionMemberSchema.getSchema(),!config.isUtf8EncodingEnabled())) {
            switchBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, List.class)
                .addStatement(
                    "return com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.get$1LList($2L, $3L)",
                    config.getDefaultMethodStringRepresentation().getJsonValue(), escapedFieldName,
                    SpecificRecordGeneratorUtil.isCollectionSchemaValuePrimitive(field.getSchema(), !config.isUtf8EncodingEnabled()))
                .endControlFlow();
          } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(unionMemberSchema.getSchema(),!config.isUtf8EncodingEnabled())) {
            switchBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, Map.class)
                .addStatement(
                    "return com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.get$1LMap($2L)",
                    config.getDefaultMethodStringRepresentation().getJsonValue(), escapedFieldName)
                .endControlFlow();
          }
        }
        switchBuilder.beginControlFlow("else")
            .addStatement("return this.$1L", escapedFieldName)
            .endControlFlow();
      } else {
        switchBuilder.addStatement("case $L: return $L", fieldIndex++, escapedFieldName);
      }
    }
    switchBuilder.addStatement("default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\")")
        .endControlFlow();

    classBuilder.addMethod(methodSpecBuilder.addCode(switchBuilder.build()).build());
  }

  private boolean unionSchemaContainsBothIntAndLong(AvroUnionSchema unionSchema) {
    boolean unionContainsInt = false;
    boolean unionContainsLong = false;
    for (SchemaOrRef unionMemberSchema : unionSchema.getTypes()) {
      if (SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.INT, unionMemberSchema.getSchema())) {
        unionContainsInt = true;
      }
      if (SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.LONG, unionMemberSchema.getSchema())) {
        unionContainsLong = true;
      }
    }
    return unionContainsInt && unionContainsLong;
  }

  private MethodSpec getOverloadedSetterSpecIfIntOrLongField(AvroSchemaField field,
      SpecificRecordGenerationConfig config) {
    MethodSpec.Builder numberSetter = null;
    String escapedFieldName = getFieldNameWithSuffix(field);
    if (SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.LONG, field.getSchema())
        || SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.INT, field.getSchema())) {

      // Get the class from the schema
      Class<?> fieldClass =
          SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(field.getSchemaOrRef().getSchema().type(),
              config.getDefaultMethodStringRepresentation(), false);

      numberSetter = MethodSpec
          .methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
          .addModifiers(Modifier.PUBLIC);

      if(fieldClass != null ) {
        if (fieldClass.equals(long.class)) {
          // If field is of type long, add method with int parameter
          // int value can safely be cast to long
          numberSetter
              .addParameter( int.class, escapedFieldName)
              .addStatement("this.$1L = (long) $1L", escapedFieldName);
        } else if (fieldClass.equals(int.class)) {
          // If field is of type int, add method with long parameter
          // This method will error if the long input is greater than Integer.MAX_VALUE

          CodeBlock castToInt = CodeBlock
              .builder()
              .beginControlFlow("if ($1L <= Integer.MAX_VALUE && $1L >= Integer.MIN_VALUE)", escapedFieldName)
              .addStatement("this.$1L = (int) $1L", escapedFieldName)
              .endControlFlow()
              .beginControlFlow("else")
              .addStatement("throw new org.apache.avro.AvroRuntimeException(\"long value cannot be cast to int\")")
              .endControlFlow()
              .build();

          numberSetter
              .addParameter(long.class, escapedFieldName)
              .addCode(castToInt);
        }

      } else if (field.getSchema() != null && field.getSchema().type().equals(AvroType.UNION)) {
        TypeName typeName = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
            field.getSchemaOrRef().getSchema().type(), true,
            config.getDefaultMethodStringRepresentation());

        CodeBlock nullCheck = CodeBlock.builder()
            .beginControlFlow("if ($1L == null)", escapedFieldName)
            .addStatement("this.$1L = null", escapedFieldName)
            .endControlFlow()
            .build();

        if (typeName.equals(ClassName.get(Long.class))) {
          numberSetter
              .addParameter(ClassName.get(Integer.class), escapedFieldName)
              .addCode(nullCheck)
              .beginControlFlow("else")
              .addStatement("this.$1L = Long.valueOf($1L)", escapedFieldName)
              .endControlFlow();

        } else if (typeName.equals(ClassName.get(Integer.class))) {

          CodeBlock castToInt = CodeBlock
              .builder()
              .beginControlFlow("else if ($1L <= Integer.MAX_VALUE && $1L >= Integer.MIN_VALUE)", escapedFieldName)
              .addStatement("this.$1L = $1L.intValue()", escapedFieldName)
              .endControlFlow()
              .beginControlFlow("else")
              .addStatement("throw new org.apache.avro.AvroRuntimeException(\"Long value cannot be cast to Integer\")")
              .endControlFlow()
              .build();

          numberSetter
              .addParameter(ClassName.get(Long.class), escapedFieldName)
              .addCode(nullCheck)
              .addCode(castToInt);
        }
      }
    }
    return numberSetter == null ? null : numberSetter.build();
  }

  private MethodSpec getOverloadedSetterSpecIfStringField(AvroSchemaField field,
      SpecificRecordGenerationConfig config) {
    MethodSpec.Builder stringSetter = null;
    String escapedFieldName = getFieldNameWithSuffix(field);
    if (config.isUtf8EncodingEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, field.getSchema())) {
      Class<?> fieldClass =
          SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(field.getSchemaOrRef().getSchema().type(),
              config.getDefaultMethodStringRepresentation().equals(AvroJavaStringRepresentation.STRING)
                  ? AvroJavaStringRepresentation.CHAR_SEQUENCE : AvroJavaStringRepresentation.STRING,
              false);
      stringSetter = MethodSpec
          .methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
          .addModifiers(Modifier.PUBLIC);

      if(fieldClass != null ) {
        if(fieldClass.equals(CharSequence.class)) {
          stringSetter.addAnnotation(Deprecated.class);
        }
        stringSetter
            .addParameter(fieldClass, escapedFieldName)
            .addModifiers(Modifier.PUBLIC);

      } else if (field.getSchema() != null && field.getSchema().type().equals(AvroType.UNION)) {
        TypeName typeName = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
            field.getSchemaOrRef().getSchema().type(), true,
            config.getDefaultMethodStringRepresentation().equals(AvroJavaStringRepresentation.STRING)
                ? AvroJavaStringRepresentation.CHAR_SEQUENCE : AvroJavaStringRepresentation.STRING);

        stringSetter.addParameter(typeName, escapedFieldName);

        if (typeName.equals(ClassName.get(CharSequence.class))) {
          stringSetter.addAnnotation(Deprecated.class);
        }
      }
      if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
        stringSetter.addStatement(
            "this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getString($1L)", escapedFieldName);
      } else {
        stringSetter.addStatement(
            "this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8($1L)", escapedFieldName);
      }
    }
    return stringSetter == null ? null : stringSetter.build();
  }

  private MethodSpec getSetterMethodSpec(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    String escapedFieldName = getFieldNameWithSuffix(field);

    MethodSpec.Builder methodSpecBuilder = MethodSpec
        .methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
        .addModifiers(Modifier.PUBLIC);

    if(field.getSchemaOrRef().getSchema() != null) {
      Class<?> fieldClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(field.getSchemaOrRef().getSchema().type(), config.getDefaultMethodStringRepresentation(), false);
      if (fieldClass != null) {
        if(fieldClass.equals(CharSequence.class)) {
          methodSpecBuilder.addAnnotation(Deprecated.class);
        }
        methodSpecBuilder.addParameter(fieldClass, escapedFieldName)
            .addModifiers(Modifier.PUBLIC);
      } else {
        TypeName typeName = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
            field.getSchemaOrRef().getSchema().type(), true, config.getDefaultMethodStringRepresentation());
        if (typeName.equals(ClassName.get(CharSequence.class))) {
          methodSpecBuilder.addAnnotation(Deprecated.class);
        }
        methodSpecBuilder.addParameter(typeName, escapedFieldName);
      }
    } else {
      methodSpecBuilder.addParameter(
          ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef()),
          escapedFieldName);
    }

    // false if field type is reference
    if (config.isUtf8EncodingEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, field.getSchema())) {
      if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
        methodSpecBuilder.addStatement(
            "this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getString($1L)", escapedFieldName);
      } else {
        methodSpecBuilder.addStatement(
            "this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8($1L)", escapedFieldName);
      }

    } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(field.getSchema(), !config.isUtf8EncodingEnabled())) {
      if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
        methodSpecBuilder.addStatement(
            "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToString($1L)",
            escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
      } else {
        methodSpecBuilder.addStatement(
            "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8($1L)",
            escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
      }
    } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(field.getSchema(), !config.isUtf8EncodingEnabled())) {
      if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
        methodSpecBuilder.addStatement(
            "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToString($1L)",
            escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
      } else {
        methodSpecBuilder.addStatement(
            "this.$1L = ($2T) com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8($1L)",
            escapedFieldName, SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()));
      }
    } else if (field.getSchema() != null && AvroType.UNION.equals(field.getSchema().type())) {
      methodSpecBuilder.beginControlFlow("if ($1L == null)", escapedFieldName)
          .addStatement("this.$1L = null", escapedFieldName)
          .endControlFlow();

      // if union might contain string value in runtime
      for (SchemaOrRef unionMemberSchema : ((AvroUnionSchema) field.getSchema()).getTypes()) {
        if (config.isUtf8EncodingEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, unionMemberSchema.getSchema())) {
          methodSpecBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, CharSequence.class);
          if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
            methodSpecBuilder.addStatement(
                "this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getString($1L)", escapedFieldName);
          } else {
            methodSpecBuilder.addStatement(
                "this.$1L = com.linkedin.avroutil1.compatibility.StringConverterUtil.getUtf8($1L)", escapedFieldName);
          }
          methodSpecBuilder.endControlFlow();

        } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(unionMemberSchema.getSchema(), !config.isUtf8EncodingEnabled())) {
          methodSpecBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, List.class);
          if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
            methodSpecBuilder.addStatement(
                "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToString($1L)",
                escapedFieldName);
          } else {
            methodSpecBuilder.addStatement(
                "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.convertToUtf8($1L)",
                escapedFieldName);
          }
          methodSpecBuilder.endControlFlow();

        } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(unionMemberSchema.getSchema(), !config.isUtf8EncodingEnabled())) {
          methodSpecBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, Map.class);
          if (config.getDefaultFieldStringRepresentation().equals(AvroJavaStringRepresentation.STRING)) {
            methodSpecBuilder.addStatement(
                "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToString($1L)",
                escapedFieldName);
          } else {
            methodSpecBuilder.addStatement(
                "this.$1L = com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.convertToUtf8($1L)",
                escapedFieldName);
          }
          methodSpecBuilder.endControlFlow();
        }
      }
      methodSpecBuilder.beginControlFlow("else")
          .addStatement("this.$1L = ($2L) $1L", escapedFieldName,
              SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
                  field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation()))
          .endControlFlow();
    } else {
      methodSpecBuilder.addStatement("this.$1L = $1L", escapedFieldName);
    }

    return methodSpecBuilder.build();
  }

  private MethodSpec getGetterMethodSpec(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    String escapedFieldName = getFieldNameWithSuffix(field);
    MethodSpec.Builder methodSpecBuilder = MethodSpec
        .methodBuilder(getMethodNameForFieldWithPrefix("get", field.getName())).addModifiers(Modifier.PUBLIC);
    TypeName typeName = null;
    if (field.getSchemaOrRef().getSchema() != null) {
      AvroType fieldType = field.getSchemaOrRef().getSchema().type();
      Class<?> fieldClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(fieldType, config.getDefaultMethodStringRepresentation(), false);
      if (fieldClass != null) {
        methodSpecBuilder.returns(fieldClass);
      } else {
        typeName = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(), fieldType, true, config.getDefaultMethodStringRepresentation());
        methodSpecBuilder.returns(typeName);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      methodSpecBuilder.returns(className);
    }
    // if fieldRepresentation != methodRepresentation for String field
    // false if field type is reference
    if (config.isUtf8EncodingEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, field.getSchema())) {
      methodSpecBuilder.addStatement(
          "return this.$1L == null ? null : com.linkedin.avroutil1.compatibility.StringConverterUtil.get$2L(this.$1L)",
          escapedFieldName, config.getDefaultMethodStringRepresentation().getJsonValue());
    } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(field.getSchema(), !config.isUtf8EncodingEnabled())) {
      methodSpecBuilder.addStatement(
          "return com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.get$LList(this.$L, $L)",
          config.getDefaultMethodStringRepresentation().getJsonValue(), escapedFieldName,
          SpecificRecordGeneratorUtil.isCollectionSchemaValuePrimitive(field.getSchema(), !config.isUtf8EncodingEnabled()));
    } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(field.getSchema(), !config.isUtf8EncodingEnabled())) {
      methodSpecBuilder.addStatement(
          "return com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.get$LMap(this.$L)",
          config.getDefaultMethodStringRepresentation().getJsonValue(),
          escapedFieldName);
    } else if (field.getSchema() != null && AvroType.UNION.equals(field.getSchema().type())) {

      methodSpecBuilder.beginControlFlow("if (this.$1L == null)", escapedFieldName)
          .addStatement("return null", escapedFieldName)
          .endControlFlow();

      // if union might contain string value in runtime
      for (SchemaOrRef unionMemberSchema : ((AvroUnionSchema) field.getSchema()).getTypes()) {
        if (config.isUtf8EncodingEnabled() && SpecificRecordGeneratorUtil.isNullUnionOf(AvroType.STRING, unionMemberSchema.getSchema())) {
          methodSpecBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, CharSequence.class)
              .addStatement("return com.linkedin.avroutil1.compatibility.StringConverterUtil.get$2L($1L)",
                  escapedFieldName, config.getDefaultMethodStringRepresentation().getJsonValue())
              .endControlFlow();
        } else if (SpecificRecordGeneratorUtil.isListTransformerApplicableForSchema(unionMemberSchema.getSchema(),!config.isUtf8EncodingEnabled())) {
          methodSpecBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, List.class)
              .addStatement(
                  "return com.linkedin.avroutil1.compatibility.collectiontransformer.ListTransformer.get$2LList($1L, $3L)",
                  escapedFieldName, config.getDefaultMethodStringRepresentation().getJsonValue(),
                  SpecificRecordGeneratorUtil.isCollectionSchemaValuePrimitive(field.getSchema(), !config.isUtf8EncodingEnabled()))
              .endControlFlow();
        } else if (SpecificRecordGeneratorUtil.isMapTransformerApplicable(unionMemberSchema.getSchema(), !config.isUtf8EncodingEnabled())) {
          methodSpecBuilder.beginControlFlow("else if($1L instanceof $2T)", escapedFieldName, Map.class)
              .addStatement(
                  "return com.linkedin.avroutil1.compatibility.collectiontransformer.MapTransformer.get$2LMap($1L)",
                  escapedFieldName, config.getDefaultMethodStringRepresentation().getJsonValue())
              .endControlFlow();
        }
      }
      methodSpecBuilder.beginControlFlow("else")
          .addStatement("return this.$1L", escapedFieldName)
          .endControlFlow();
    } else {
      if(typeName != null) {
        methodSpecBuilder.addStatement("return ($T)this.$L", typeName, escapedFieldName);
      } else {
        methodSpecBuilder.addStatement("return this.$L", escapedFieldName);
      }

    }

    return methodSpecBuilder.build();
  }

  private FieldSpec.Builder getFieldSpecBuilder(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    FieldSpec.Builder fieldSpecBuilder;
    String escapedFieldName = getFieldNameWithSuffix(field);
    if(field.getSchemaOrRef().getSchema() != null) {
      Class<?> fieldClass = SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(field.getSchemaOrRef().getSchema().type(), config.getDefaultFieldStringRepresentation(), false);
      if (fieldClass != null) {
        fieldSpecBuilder = FieldSpec.builder(fieldClass, escapedFieldName);
      } else {
        TypeName className = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
            field.getSchemaOrRef().getSchema().type(), true, config.getDefaultFieldStringRepresentation());
        fieldSpecBuilder = FieldSpec.builder(className, escapedFieldName);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      fieldSpecBuilder = FieldSpec.builder(className, escapedFieldName);
    }
    return fieldSpecBuilder;
  }

  private ParameterSpec getParameterSpecForField(AvroSchemaField field, AvroJavaStringRepresentation stringRepresentation) {
    final ParameterSpec.Builder parameterSpecBuilder;
    String escapedFieldName = getFieldNameWithSuffix(field);
    if(field.getSchemaOrRef().getSchema() != null) {
      //TODO : Input validation for non-nullable fields since Boxed types are used for Constructors
      Class<?> fieldClass =
          SpecificRecordGeneratorUtil.getJavaClassForAvroTypeIfApplicable(field.getSchemaOrRef().getSchema().type(),
              stringRepresentation, true);
      if (fieldClass != null) {
          parameterSpecBuilder = ParameterSpec.builder(fieldClass, escapedFieldName);
      } else {
        TypeName typeName = SpecificRecordGeneratorUtil.getTypeName(field.getSchemaOrRef().getSchema(),
            field.getSchemaOrRef().getSchema().type(), true, stringRepresentation);
        parameterSpecBuilder = ParameterSpec.builder(typeName, escapedFieldName);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      parameterSpecBuilder = ParameterSpec.builder(className, escapedFieldName);
    }
    return parameterSpecBuilder.build();
  }

  private void addAndInitializeSizeFieldToClass(TypeSpec.Builder classBuilder, AvroFixedSchema fixedSchema)
      throws ClassNotFoundException {
    classBuilder.addAnnotation(AnnotationSpec.builder(SpecificRecordGeneratorUtil.CLASSNAME_FIXED_SIZE)
        .addMember("value", CodeBlock.of(String.valueOf(fixedSchema.getSize())))
        .build());
  }

  /**
   * adds "public final static Schema SCHEMA$" field to generated classes for named avro types.
   * the field is defined as:
   * public final static Schema SCHEMA$ =
   *    com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.parse(avsc1, avsc2, avsc3 ...)
   * where the arguments are pieces of the input schema's self-contained (fully-inlined) avsc
   * representation. java does not allow string literals to be &gt; 64K in size, so large avsc literals
   * are chunked and the var-args Helper.parse() is used.
   * @param classBuilder builder for a class being generated
   * @param classSchema schema of the class being generated
   */
  protected void addSchema$ToGeneratedClass(TypeSpec.Builder classBuilder, AvroNamedSchema classSchema) {
    ClassName avroSchemaType = SpecificRecordGeneratorUtil.CLASSNAME_SCHEMA;
    classBuilder.alwaysQualify(avroSchemaType.simpleName()); //no import statements

    //get fully-inlined single-line avsc from schema
    AvscSchemaWriter avscWriter = new AvscSchemaWriter();
    String avsc = avscWriter.writeSingle(classSchema).getContents();

    //JVM spec says string literals cant be over 65535 bytes in size (this isnt simply the
    //character count as horrible wide unicode characters could be involved).
    //for details see https://docs.oracle.com/javase/specs/jvms/se8/html/jvms-4.html#jvms-4.4.7
    //we add some extra safety margin
    String parseFormat;
    Object[] parseFormatArgs;
    if (avsc.getBytes(StandardCharsets.UTF_8).length > 64000) {
      //not 100% safe as argument is in characters and should be bytes ...
      List<String> chunks = SourceCodeUtils.safeSplit(avsc, 20000);
      StringJoiner csv = new StringJoiner(", ");
      for (int i = 1; i <= chunks.size(); i++) {
        //"$1S, $2S, ... $NS"
        csv.add("$" + i + "S");
      }
      parseFormat = HelperConsts.HELPER_FQCN + ".parse(" + csv + ")";
      parseFormatArgs = chunks.toArray(new Object[] {});
    } else {
      //no need to split anything
      parseFormat = HelperConsts.HELPER_FQCN + ".parse($1S)";
      parseFormatArgs = new Object[] {avsc};
    }
    classBuilder.addField(FieldSpec
            .builder(avroSchemaType, "SCHEMA$", Modifier.PUBLIC, Modifier.FINAL, Modifier.STATIC)
            //TODO - use strict parsing
            .initializer(CodeBlock.of(parseFormat, parseFormatArgs))
            .build()
    );
  }
}
