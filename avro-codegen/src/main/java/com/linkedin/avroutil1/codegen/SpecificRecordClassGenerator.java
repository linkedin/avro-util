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
import com.linkedin.avroutil1.compatibility.StringConverterUtil;
import com.linkedin.avroutil1.compatibility.StringUtils;
import com.linkedin.avroutil1.compatibility.exception.AvroUtilException;
import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroJavaStringRepresentation;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroName;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.writer.avsc.AvscSchemaWriter;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.tools.JavaFileObject;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.util.Utf8;



/**
 * generates java classes out of avro schemas.
 */
public class SpecificRecordClassGenerator {

  /***
   * Pattern to match single instance of $ sign
   */
  private final Pattern SINGLE_DOLLAR_SIGN_REGEX = Pattern.compile("(?<![\\$])[\\$](?![\\$])");

  private static final String AVRO_GEN_COMMENT = "GENERATED CODE by avro-util";

  final ClassName CLASSNAME_SCHEMA = ClassName.get("org.apache.avro", "Schema");
  final ClassName CLASSNAME_SPECIFIC_DATA = ClassName.get("org.apache.avro.specific", "SpecificData");
  final ClassName CLASSNAME_SPECIFIC_RECORD = ClassName.get("org.apache.avro.specific", "SpecificRecord");
  final ClassName CLASSNAME_SPECIFIC_RECORD_BASE = ClassName.get("org.apache.avro.specific", "SpecificRecordBase");
  final ClassName CLASSNAME_SPECIFIC_DATUM_READER = ClassName.get("org.apache.avro.specific", "SpecificDatumReader");
  final ClassName CLASSNAME_SPECIFIC_DATUM_WRITER = ClassName.get("org.apache.avro.specific", "SpecificDatumWriter");
  final ClassName CLASSNAME_ENCODER = ClassName.get("org.apache.avro.io", "Encoder");
  final ClassName CLASSNAME_RESOLVING_DECODER = ClassName.get("org.apache.avro.io", "ResolvingDecoder");
  final ClassName CLASSNAME_DATUM_READER = ClassName.get("org.apache.avro.io", "DatumReader");
  final ClassName CLASSNAME_DATUM_WRITER = ClassName.get("org.apache.avro.io", "DatumWriter");
  final ClassName CLASSNAME_FIXED_SIZE = ClassName.get("org.apache.avro.specific", "FixedSize");

  private int sizeValCounter = 0;

  HashSet<TypeName> fullyQualifiedClassNamesInRecord = new HashSet<>();
  HashSet<String> fullyQualifiedClassesInRecord = new HashSet<>(Arrays.asList(
      CLASSNAME_DATUM_READER.canonicalName(),
      CLASSNAME_DATUM_WRITER.canonicalName(),
      CLASSNAME_ENCODER.canonicalName(),
      CLASSNAME_RESOLVING_DECODER.canonicalName(),
      CLASSNAME_SPECIFIC_DATA.canonicalName(),
      CLASSNAME_SPECIFIC_DATUM_READER.canonicalName(),
      CLASSNAME_SPECIFIC_DATUM_WRITER.canonicalName(),
      CLASSNAME_SPECIFIC_RECORD.canonicalName(),
      CLASSNAME_SPECIFIC_RECORD_BASE.canonicalName(),
      IOException.class.getName(),
      Exception.class.getName(),
      ObjectInput.class.getName(),
      ObjectOutput.class.getName(),
      String.class.getName(),
      Object.class.getName(),
      ConcurrentModificationException.class.getName(),
      IllegalArgumentException.class.getName(),
      IndexOutOfBoundsException.class.getName(),
      HashMap.class.getName(),
      CompatibleSpecificRecordBuilderBase.class.getName()
  ));

  /***
   * Generates Java class for top level schema.
   *
   * @param topLevelSchema
   * @param config
   * @return Java class of top level schema
   * @throws ClassNotFoundException
   */
  public JavaFileObject generateSpecificClass(AvroNamedSchema topLevelSchema,
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

  /***
   * Generates Java classes for top level schemas and all internally defined types, excludes references.
   *
   * @param topLevelSchema
   * @param config
   * @return List of Java files
   * @throws ClassNotFoundException
   */
  public List<JavaFileObject> generateSpecificClassWithInternalTypes(AvroNamedSchema topLevelSchema,
      SpecificRecordGenerationConfig config) throws ClassNotFoundException {

    AvroType type = topLevelSchema.type();
    switch (type) {
      case ENUM:
      case FIXED:
        return Arrays.asList(generateSpecificClass(topLevelSchema, config));
      case RECORD:
        List<JavaFileObject> namedSchemaFiles = new ArrayList<>();
        populateJavaFilesOfInnerNamedSchemasFromRecord((AvroRecordSchema) topLevelSchema, config, namedSchemaFiles);
        namedSchemaFiles.add(generateSpecificRecord((AvroRecordSchema) topLevelSchema, config));
        return namedSchemaFiles;
      default:
        throw new IllegalArgumentException("cant generate java class for " + type);
    }
  }

  /***
   * Runs through internally defined schemas and generates their file objects
   *
   */
  private void populateJavaFilesOfInnerNamedSchemasFromRecord(AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config, List<JavaFileObject> namedSchemaFiles) throws ClassNotFoundException {

    Queue<AvroSchema> schemaQueue = recordSchema.getFields()
        .stream()
        .filter(field -> field.getSchemaOrRef().getRef() == null)
        .map(AvroSchemaField::getSchema)
        .collect(Collectors.toCollection(LinkedList::new));

    while (!schemaQueue.isEmpty()) {
      AvroSchema fieldSchema = schemaQueue.poll();

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
      }
    }
  }


  protected JavaFileObject generateSpecificEnum(AvroEnumSchema enumSchema, SpecificRecordGenerationConfig config) {
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


    //create file object
    TypeSpec classSpec = classBuilder.build();
    JavaFile javaFile = JavaFile.builder(enumSchema.getNamespace(), classSpec)
        .skipJavaLangImports(false) //no imports
        .addFileComment(AVRO_GEN_COMMENT)
        .build();

    return javaFile.toJavaFileObject();
  }


  protected JavaFileObject generateSpecificFixed(AvroFixedSchema fixedSchema, SpecificRecordGenerationConfig config)
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


    //add size annotation to class
    addAndInitializeSizeFieldToClass(classBuilder, fixedSchema);

    //create file object
    TypeSpec classSpec = classBuilder.build();
    JavaFile javaFile = JavaFile.builder(fixedSchema.getNamespace(), classSpec)
        .skipJavaLangImports(false) //no imports
        .addFileComment(AVRO_GEN_COMMENT)
        .build();

    return javaFile.toJavaFileObject();
  }

  protected JavaFileObject generateSpecificRecord(AvroRecordSchema recordSchema, SpecificRecordGenerationConfig config)
      throws ClassNotFoundException {

    // Default to broad compatibility config if null
    if(config == null) {
      config = SpecificRecordGenerationConfig.BROAD_COMPATIBILITY;
    }

    //public class
    TypeSpec.Builder classBuilder = TypeSpec.classBuilder(recordSchema.getSimpleName());
    classBuilder.addModifiers(Modifier.PUBLIC);

    //implements
    classBuilder.addSuperinterface(CLASSNAME_SPECIFIC_RECORD);

    // extends
    classBuilder.superclass(CLASSNAME_SPECIFIC_RECORD_BASE);

    //add class-level doc from schema doc
    //file-level (top of file) comment is added to the file object later
    String doc = recordSchema.getDoc();
    if (doc != null && !doc.isEmpty()) {
      doc = replaceSingleDollarSignWithDouble(doc);
      classBuilder.addJavadoc(doc);
    }

    if(config.getMinimumSupportedAvroVersion().laterThan(AvroVersion.AVRO_1_7)) {
      // MODEL$ as new instance of SpecificData()
      classBuilder.addField(
          FieldSpec.builder(CLASSNAME_SPECIFIC_DATA, "MODEL$", Modifier.PRIVATE,
              Modifier.STATIC)
              .initializer(CodeBlock.of("new $T()", CLASSNAME_SPECIFIC_DATA))
              .build());
    } else {
      classBuilder.addField(
          FieldSpec.builder(CLASSNAME_SPECIFIC_DATA, "MODEL$", Modifier.PRIVATE,
              Modifier.STATIC)
              .initializer(CodeBlock.of("$T.get()", CLASSNAME_SPECIFIC_DATA))
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
        .returns(CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    classBuilder.addMethod(MethodSpec.methodBuilder("getClassSchema")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(CLASSNAME_SCHEMA)
        .addStatement("return $L", "SCHEMA$")
        .build());

    if(config.getMinimumSupportedAvroVersion().laterThan(AvroVersion.AVRO_1_7)) {
      // read external
      classBuilder.addField(
          FieldSpec.builder(CLASSNAME_DATUM_READER, "READER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
              .initializer(CodeBlock.of("new $T($L)", CLASSNAME_SPECIFIC_DATUM_READER, "SCHEMA$"))
              .build());

      MethodSpec.Builder readExternalBuilder = MethodSpec.methodBuilder("readExternal")
          .addException(IOException.class)
          .addParameter(java.io.ObjectInput.class, "in")
          .addModifiers(Modifier.PUBLIC)
          .addCode(CodeBlock.builder().addStatement("$L.read(this, $T.getDecoder(in))", "READER$", CLASSNAME_SPECIFIC_DATA).build());

      // write external
      classBuilder.addField(
          FieldSpec.builder(CLASSNAME_DATUM_WRITER, "WRITER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
              .initializer(CodeBlock.of("new $T($L)", CLASSNAME_SPECIFIC_DATUM_WRITER, "SCHEMA$"))
              .build());

      MethodSpec.Builder writeExternalBuilder = MethodSpec
          .methodBuilder("writeExternal")
          .addException(IOException.class)
          .addParameter(java.io.ObjectOutput.class, "out")
          .addModifiers(Modifier.PUBLIC)
          .addCode(CodeBlock
              .builder()
              .addStatement("$L.write(this, $T.getEncoder(out))", "WRITER$", CLASSNAME_SPECIFIC_DATA)
              .build());
      readExternalBuilder.addAnnotation(Override.class);
      writeExternalBuilder.addAnnotation(Override.class);

      classBuilder.addMethod(readExternalBuilder.build());
      classBuilder.addMethod(writeExternalBuilder.build());
    }


    // add no arg constructor
    classBuilder.addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PUBLIC).build());


    if (recordSchema.getFields().size() > 0) {

      // add all arg constructor if #args < 254
      if(recordSchema.getFields().size() < 254) {
        MethodSpec.Builder allArgsConstructorBuilder = MethodSpec.constructorBuilder();
        for (AvroSchemaField field : recordSchema.getFields()) {
          //if declared schema, use fully qualified class (no import)
          String escapedFieldName = getFieldNameWithSuffix(field);
          addFullyQualified(field, config);
          allArgsConstructorBuilder.addParameter(getParameterSpecForField(field, config))
              .addStatement("this.$1L = $1L", escapedFieldName);
        }
        classBuilder.addMethod(allArgsConstructorBuilder.build());
      }

      // Add public/private fields
      Modifier accessModifier = (config.hasPublicFields())? Modifier.PUBLIC : Modifier.PRIVATE;
      for (AvroSchemaField field : recordSchema.getFields()) {
        classBuilder.addField(getFieldSpecBuilder(field, config).addModifiers(accessModifier).build());

        //if declared schema, use fully qualified class (no import)
        addFullyQualified(field, config);

        //getters
        classBuilder.addMethod(getGetterMethodSpec(field, config));

        // setters
        classBuilder.addMethod(getSetterMethodSpec(field, config));
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
            .addStatement("return $L", hasCustomCoders(recordSchema))
            .build());
    //customCoders
    if(hasCustomCoders(recordSchema)){

      // customEncode
      MethodSpec.Builder customEncodeBuilder = MethodSpec
          .methodBuilder("customEncode")
          .addParameter(CLASSNAME_ENCODER, "out")
          .addException(IOException.class)
          .addModifiers(Modifier.PUBLIC);
      addCustomEncodeMethod(customEncodeBuilder, recordSchema, config);
      classBuilder.addMethod(customEncodeBuilder.build());

      //customDecode
      MethodSpec.Builder customDecodeBuilder = MethodSpec
          .methodBuilder("customDecode")
          .addParameter(CLASSNAME_RESOLVING_DECODER, "in")
          .addException(IOException.class)
          .addModifiers(Modifier.PUBLIC);
      addCustomDecodeMethod(customDecodeBuilder, recordSchema, config);
      classBuilder.addMethod(customDecodeBuilder.build());
    }

    // Builder
    TypeSpec.Builder recordBuilder = TypeSpec.classBuilder("Builder");
    recordBuilder.addModifiers(Modifier.PUBLIC, Modifier.STATIC);
    try {
      populateBuilderClassBuilder(recordBuilder, recordSchema, config);
      classBuilder.addType(recordBuilder.build());
    } catch (ClassNotFoundException e) {
      throw new ClassNotFoundException("Exception while creating Builder: %s", e);
    }

    addDefaultFullyQualifiedClassesForSpecificRecord(classBuilder, recordSchema);

    //create file object
    TypeSpec classSpec = classBuilder.build();
    JavaFile javaFile = JavaFile.builder(recordSchema.getNamespace(), classSpec)
        .skipJavaLangImports(false) //no imports
        .addFileComment(AVRO_GEN_COMMENT)
        .build();

    return javaFile.toJavaFileObject();
  }

  private String replaceSingleDollarSignWithDouble(String str) {
    if(str != null && !str.isEmpty() && str.contains("$")) {
      str = SINGLE_DOLLAR_SIGN_REGEX.matcher(str).replaceAll("\\$\\$");
    }
    return str;
  }

  private void populateBuilderClassBuilder(TypeSpec.Builder recordBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) throws ClassNotFoundException {
    boolean canThrowMissingFieldException = false;
    recordBuilder.superclass(ClassName.get(CompatibleSpecificRecordBuilderBase.class));
    CodeBlock.Builder otherBuilderConstructorFromRecordBlockBuilder = CodeBlock.builder();
    CodeBlock.Builder otherBuilderConstructorFromOtherBuilderBlockBuilder = CodeBlock.builder();
    CodeBlock.Builder buildMethodCodeBlockBuilder = CodeBlock.builder()
        .beginControlFlow("try")
        .addStatement("$1L record = new $1L()", recordSchema.getName().getSimpleName());

    List<MethodSpec> accessorMethodSpecs = new ArrayList<>();
    int fieldIndex = 0;
    // All private fields
    for (AvroSchemaField field : recordSchema.getFields()) {
      FieldSpec.Builder fieldBuilder;
      String escapedFieldName = getFieldNameWithSuffix(field);
      AvroSchema fieldSchema = field.getSchemaOrRef().getSchema();
      AvroType fieldAvroType = fieldSchema.type();
      Class<?> fieldClass = getJavaClassForAvroTypeIfApplicable(fieldAvroType);
      TypeName fieldType = getTypeName(field.getSchema(), fieldAvroType);
      if (fieldClass != null) {
        fieldBuilder = FieldSpec.builder(fieldClass, escapedFieldName, Modifier.PRIVATE);
        buildMethodCodeBlockBuilder.addStatement(
            "record.$1L = fieldSetFlags()[$2L] ? this.$1L : ($3T) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
            escapedFieldName, fieldIndex, fieldClass);
      } else {
        fieldBuilder = FieldSpec.builder(fieldType, escapedFieldName, Modifier.PRIVATE);
        if(!AvroType.RECORD.equals(fieldAvroType)) {
          buildMethodCodeBlockBuilder.addStatement(
              "record.$1L = fieldSetFlags()[$2L] ? this.$1L : ($3L) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
              escapedFieldName, fieldIndex, fieldType);
        } else {
          canThrowMissingFieldException = true;
          buildMethodCodeBlockBuilder.beginControlFlow("if ($L != null)", escapedFieldName + "Builder")
              .beginControlFlow("try")
              .addStatement("record.$1L = this.$1LBuilder.build()", escapedFieldName)
              .endControlFlow();



          if (config.getMinimumSupportedAvroVersion().laterThan(AvroVersion.AVRO_1_8)) {
            buildMethodCodeBlockBuilder.beginControlFlow("catch (org.apache.avro.AvroMissingFieldException e)")
                .addStatement("com.linkedin.avroutil1.compatibility.exception.AvroUtilMissingFieldException avroUtilException = new com.linkedin.avroutil1.compatibility.exception.AvroUtilMissingFieldException(e)")
                .addStatement("avroUtilException.addParentField(record.getSchema().getField($S))", escapedFieldName)
                .addStatement("throw avroUtilException")
                .endControlFlow();
          }

          buildMethodCodeBlockBuilder.beginControlFlow("catch (org.apache.avro.AvroRuntimeException e)")
              .addStatement("com.linkedin.avroutil1.compatibility.exception.AvroUtilMissingFieldException avroUtilException = new com.linkedin.avroutil1.compatibility.exception.AvroUtilMissingFieldException(e)")
              .addStatement("avroUtilException.addParentField(record.getSchema().getField($S))", escapedFieldName)
              .addStatement("throw avroUtilException")
              .endControlFlow()
              .endControlFlow()
              .beginControlFlow("else")
              .addStatement(
              "record.$1L = fieldSetFlags()[$2L] ? this.$1L : ($3L) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
              escapedFieldName, fieldIndex, fieldType)
          .endControlFlow();
        }
      }
      if (field.hasDoc()) {
        fieldBuilder.addJavadoc(replaceSingleDollarSignWithDouble(field.getDoc()));
      }
      recordBuilder.addField(fieldBuilder.build());

      otherBuilderConstructorFromRecordBlockBuilder.beginControlFlow("if (isValidValue(fields()[$L], other.$L))", fieldIndex,
          escapedFieldName)
          .addStatement("this.$1L = deepCopyField(other.$1L, fields()[$2L].schema(), $3S)", escapedFieldName, fieldIndex,
              config.getDefaultFieldStringRepresentation().getJsonValue())
          .addStatement("fieldSetFlags()[$L] = true", fieldIndex)
          .endControlFlow();

      otherBuilderConstructorFromOtherBuilderBlockBuilder.beginControlFlow("if (isValidValue(fields()[$L], other.$L))", fieldIndex,
          escapedFieldName)
          .addStatement("this.$1L = deepCopyField(other.$1L, fields()[$2L].schema(), $3S)", escapedFieldName, fieldIndex,
              config.getDefaultFieldStringRepresentation().getJsonValue())
          .addStatement("fieldSetFlags()[$1L] = other.fieldSetFlags()[$1L]", fieldIndex)
          .endControlFlow();

      if (AvroType.RECORD.equals(fieldAvroType)) {
        recordBuilder.addField(
            FieldSpec.builder(ClassName.get(((AvroRecordSchema) field.getSchema()).getFullName(), "Builder"),
                escapedFieldName + "Builder").build());

        populateRecordBuilderAccessor(accessorMethodSpecs, escapedFieldName, recordSchema.getFullName(), field);

        otherBuilderConstructorFromRecordBlockBuilder.addStatement("this.$L = null", escapedFieldName+"Builder");
        otherBuilderConstructorFromOtherBuilderBlockBuilder.beginControlFlow("if (other.$L())",
            getMethodNameForFieldWithPrefix("has", escapedFieldName + "Builder"))
            .addStatement("this.$L = $L.Builder.newBuilder(other.$L())", escapedFieldName + "Builder",
                ((AvroRecordSchema) field.getSchema()).getFullName(),
                getMethodNameForFieldWithPrefix("get", escapedFieldName + "Builder"))
        .endControlFlow();

      }

      // get, set, has, clear methods
      populateAccessorMethodsBlock(accessorMethodSpecs, field, fieldClass, fieldType, recordSchema.getFullName(),
          fieldIndex);

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

    //new Builder methods
    recordBuilder.addMethods(getNewBuilderMethods(recordSchema));

    //Build method
    //try
    buildMethodCodeBlockBuilder
        .addStatement("return record")
        .endControlFlow();

    if(canThrowMissingFieldException) {
      buildMethodCodeBlockBuilder.beginControlFlow(
              "catch (com.linkedin.avroutil1.compatibility.exception.AvroUtilMissingFieldException e)")
          .addStatement("throw e")
          .endControlFlow();
    }

    buildMethodCodeBlockBuilder
        .beginControlFlow("catch ($T e)", Exception.class)
        .addStatement("throw new com.linkedin.avroutil1.compatibility.exception.AvroUtilException(e)")
        .endControlFlow();

    recordBuilder.addMethod(
        MethodSpec.methodBuilder("build")
            .addModifiers(Modifier.PUBLIC)
            .addException(AvroUtilException.class)
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

  private void populateRecordBuilderAccessor(List<MethodSpec> accessorMethodSpecs, String fieldName,
      String recordFullName, AvroSchemaField field) {

    String builderName = fieldName+"Builder";
    String fieldParentNamespace = ((AvroRecordSchema)field.getSchema()).getFullName();
    // Getter
    MethodSpec.Builder getMethodBuilder = MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("get", builderName))
        .addModifiers(Modifier.PUBLIC)
        .returns(ClassName.get(fieldParentNamespace, "Builder"))
        .addJavadoc("Gets the Builder instance for the '$L' field and creates one if it doesn't exist yet.\n@return This builder.", fieldName)
        .beginControlFlow("if ($L == null )", builderName)
        .beginControlFlow("if($L())", getMethodNameForFieldWithPrefix("has", fieldName))
        .addStatement("$L($L.Builder.newBuilder($L))", getMethodNameForFieldWithPrefix("set", builderName),
            fieldParentNamespace, fieldName)
        .endControlFlow()
        .beginControlFlow("else")
        .addStatement("$L($L.Builder.newBuilder())", getMethodNameForFieldWithPrefix("set", builderName),
            fieldParentNamespace)
        .endControlFlow()
        .endControlFlow()
        .addStatement("return $L", builderName);

    // Setter
    MethodSpec.Builder setMethodBuilder = MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("set", builderName))
        .addModifiers(Modifier.PUBLIC)
        .returns(ClassName.get(recordFullName, "Builder"))
        .addJavadoc("Sets the Builder instance for the '$L' field\n "
            + "@param value The builder instance that must be set."
            + "\n@return This builder.", fieldName)
        .addParameter(ClassName.get(fieldParentNamespace, "Builder"), "value")
        .addStatement("$L()", getMethodNameForFieldWithPrefix("clear", fieldName))
        .addStatement("$L = value", builderName)
        .addStatement("return this");

    // Has
    MethodSpec.Builder hasMethodBuilder = MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("has", builderName))
        .addModifiers(Modifier.PUBLIC)
        .returns(boolean.class)
        .addJavadoc("Checks whether the '$1L' field has an active Builder instance\n"
            + "@return True if the '$1L' field has an active Builder instance", fieldName)
        .addStatement("return $L != null", builderName);

    accessorMethodSpecs.add(getMethodBuilder.build());
    accessorMethodSpecs.add(setMethodBuilder.build());
    accessorMethodSpecs.add(hasMethodBuilder.build());
  }

  private void populateAccessorMethodsBlock(List<MethodSpec> accessorMethodSpecs, AvroSchemaField field,
      Class<?> fieldClass, TypeName fieldType, String parentClass, int fieldIndex) {
    String escapedFieldName = getFieldNameWithSuffix(field);
    //Getter
    MethodSpec.Builder getMethodBuilder =
        MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("get", escapedFieldName))
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Gets the value of the '$L' field.$L" + "@return The value.", field.getName(),
                getFieldJavaDoc(field))
        .addStatement("return $L", escapedFieldName);
    if(fieldClass != null) {
      getMethodBuilder.returns(fieldClass);
    } else {
      getMethodBuilder.returns(fieldType);
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
                field.getName(), getFieldJavaDoc(field))
            .addStatement("$L = null", escapedFieldName)
            .addStatement("fieldSetFlags()[$L] = false", fieldIndex)
            .addStatement("return this")
            .returns(ClassName.get(parentClass, "Builder"));

    accessorMethodSpecs.add(getMethodBuilder.build());
    accessorMethodSpecs.add(setMethodBuilder.build());
    accessorMethodSpecs.add(hasMethodBuilder.build());
    accessorMethodSpecs.add(clearMethodBuilder.build());
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
    if (fieldName.length() < 1) {
      throw new IllegalArgumentException("FieldName must be longer than 1");
    }

    return prefix + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1)
        + getSuffixForFieldName(fieldName);
  }

  private void addCustomDecodeMethod(MethodSpec.Builder customDecodeBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) {
    // reset var counter
    sizeValCounter = 0;
    customDecodeBuilder.addStatement("org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrder()")
        .beginControlFlow("if (fieldOrder == null)");
    for(AvroSchemaField field : recordSchema.getFields()) {
      String escapedFieldName = getFieldNameWithSuffix(field);
      customDecodeBuilder.addStatement(getSerializedCustomDecodeBlock(config, field.getSchemaOrRef().getSchema(),
          field.getSchemaOrRef().getSchema().type(), replaceSingleDollarSignWithDouble(escapedFieldName)));
    }
    // reset var counter
    sizeValCounter = 0;
    int fieldIndex = 0;
    customDecodeBuilder.endControlFlow()
        .beginControlFlow("else")
        .beginControlFlow("for( int i = 0; i< $L; i++)", recordSchema.getFields().size())
        .beginControlFlow("switch(fieldOrder[i].pos())");
    for(AvroSchemaField field : recordSchema.getFields()) {
      String escapedFieldName = getFieldNameWithSuffix(field);
      customDecodeBuilder
          .addStatement(String.format("case %s: ",fieldIndex++)+ getSerializedCustomDecodeBlock(config,
              field.getSchemaOrRef().getSchema(), field.getSchemaOrRef().getSchema().type(), replaceSingleDollarSignWithDouble(escapedFieldName)))
          .addStatement("break");
    }
    customDecodeBuilder
        //switch
        .endControlFlow()
        //for
        .endControlFlow()
        //else
        .endControlFlow();

  }


  private String getSerializedCustomDecodeBlock(SpecificRecordGenerationConfig config,
      AvroSchema fieldSchema, AvroType fieldType, String fieldName) {
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
        serializedCodeBlock = String.format("%s = in.readInt()", fieldName);
        break;
      case FLOAT:
        serializedCodeBlock = String.format("%s = in.readFloat()", fieldName);
        break;
      case LONG:
        serializedCodeBlock = String.format("%s = in.readLong()", fieldName);
        break;
      case DOUBLE:
        serializedCodeBlock = String.format("%s = in.readDouble()", fieldName);
        break;
      case BYTES:
        serializedCodeBlock = String.format("%s = in.readBytes(this.%s)", fieldName, fieldName);
        break;
      case STRING:
        serializedCodeBlock =
            String.format("%s = in.readString(%s instanceof org.apache.avro.util.Utf8 ? (org.apache.avro.util.Utf8)%s : null)", fieldName, fieldName, fieldName);
        break;
      case ENUM:
        TypeName enumClassName = getTypeName(fieldSchema, AvroType.ENUM);
        serializedCodeBlock =
            String.format("%s = %s.values()[in.readEnum()]", fieldName, enumClassName.toString());
        break;
      case FIXED:
        codeBlockBuilder.beginControlFlow("if ($L == null)", fieldName)
            .addStatement("$L = new $L()", fieldName,
                getTypeName(fieldSchema, AvroType.FIXED).toString())
            .endControlFlow().addStatement("in.readFixed($L.bytes(), 0, $L)", fieldName,
            ((AvroFixedSchema) fieldSchema).getSize());

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
      case ARRAY:

        String arrayVarName = getArrayVarName();
        String gArrayVarName = getGArrayVarName();
        AvroSchema arrayItemSchema = ((AvroArraySchema) fieldSchema).getValueSchema();
        Class<?> arrayItemClass = getJavaClassForAvroTypeIfApplicable(arrayItemSchema.type());
        TypeName arrayItemTypeName = getTypeName(arrayItemSchema, arrayItemSchema.type());

        codeBlockBuilder
            .addStatement("long $L = in.readArrayStart()", getSizeVarName())
            .addStatement("$T<$T> $L = $L", List.class, arrayItemClass != null ? arrayItemClass : arrayItemTypeName, arrayVarName, fieldName)
            .beginControlFlow("if($L == null)", arrayVarName)
            .addStatement("$L = new org.apache.avro.specific.SpecificData.Array<$T>((int)$L, $L.getField(\"nullArrayField\").schema())",
                arrayVarName, arrayItemClass != null ? arrayItemClass : arrayItemTypeName, getSizeVarName(), "SCHEMA$$")
            .addStatement("$L = $L", fieldName, arrayVarName)
            .endControlFlow()
            .beginControlFlow("else")
            .addStatement("$L.clear()", arrayVarName)
            .endControlFlow();

        codeBlockBuilder.addStatement(
            "org.apache.avro.specific.SpecificData.Array<$T> $L = ($L instanceof org.apache.avro.specific.SpecificData.Array ? (org.apache.avro.specific.SpecificData.Array<$T>)$L : null)",
            arrayItemClass != null ? arrayItemClass : arrayItemTypeName, gArrayVarName, arrayVarName, arrayItemClass != null ? arrayItemClass : arrayItemTypeName, arrayVarName);
        codeBlockBuilder.beginControlFlow("for (; 0 < $L; $L = in.arrayNext())", getSizeVarName(), getSizeVarName())
            .beginControlFlow("for(; $L != 0; $L--)", getSizeVarName(), getSizeVarName())
            .addStatement("$T $L = ($L != null ? $L.peek() : null)", arrayItemClass != null ? arrayItemClass : arrayItemTypeName, getElementVarName(), gArrayVarName, gArrayVarName);

        codeBlockBuilder.addStatement(
            getSerializedCustomDecodeBlock(config, arrayItemSchema, arrayItemSchema.type(), getElementVarName()));
        codeBlockBuilder.addStatement("$L.add($L)", arrayVarName, getElementVarName())
            .endControlFlow()
            .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();

        sizeValCounter++;

        break;
      case MAP:

        String mapVarName = getMapVarName();
        AvroType mapItemAvroType = ((AvroMapSchema) fieldSchema).getValueSchema().type();
        Class<?> mapItemClass = getJavaClassForAvroTypeIfApplicable(mapItemAvroType);
        TypeName mapItemClassName = getTypeName(((AvroMapSchema) fieldSchema).getValueSchema(), mapItemAvroType);

        codeBlockBuilder
            .addStatement("long $L = in.readMapStart()", getSizeVarName());

        codeBlockBuilder.addStatement("$T<$T,$T> $L = $L", Map.class, CharSequence.class,
            ((mapItemClass != null) ? mapItemClass : mapItemClassName), mapVarName, fieldName);

        codeBlockBuilder.beginControlFlow("if($L == null)", mapVarName)
          .addStatement("$L = new $T<$T,$T>((int)$L)", mapVarName, HashMap.class, CharSequence.class,
              ((mapItemClass != null) ? mapItemClass : mapItemClassName), getSizeVarName())
          .addStatement("$L = $L", fieldName, mapVarName)
          .endControlFlow()
          .beginControlFlow("else")
          .addStatement("$L.clear()", mapVarName)
          .endControlFlow();

        codeBlockBuilder.beginControlFlow("for (; 0 < $L; $L = in.mapNext())", getSizeVarName(), getSizeVarName())
            .beginControlFlow("for(; $L != 0; $L--)", getSizeVarName(), getSizeVarName())
            .addStatement("$T $L = null", CharSequence.class, getKeyVarName())
            .addStatement(getSerializedCustomDecodeBlock(config, ((AvroMapSchema) fieldSchema).getValueSchema(), AvroType.STRING, getKeyVarName()))
            .addStatement("$T $L = null", ((mapItemClass != null) ? mapItemClass : mapItemClassName), getValueVarName())
            .addStatement(
                getSerializedCustomDecodeBlock(config, ((AvroMapSchema) fieldSchema).getValueSchema(),
                    ((AvroMapSchema) fieldSchema).getValueSchema().type(), getValueVarName()));

        codeBlockBuilder.addStatement("$L.put($L,$L)", mapVarName, getKeyVarName(), getValueVarName())
            .endControlFlow()
            .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        sizeValCounter++;

        break;

      case UNION:
        int numberOfUnionMembers = ((AvroUnionSchema) fieldSchema).getTypes().size();
        for (int i = 0; i < numberOfUnionMembers; i++) {

          SchemaOrRef unionMember = ((AvroUnionSchema) fieldSchema).getTypes().get(i);
          if (i == 0) {
            codeBlockBuilder.beginControlFlow("if (in.readIndex() == $L) ", i);
          } else {
            codeBlockBuilder.beginControlFlow(" else if (in.readIndex() == $L) ", i);
          }
          codeBlockBuilder.addStatement(
              getSerializedCustomDecodeBlock(config, unionMember.getSchema(), unionMember.getSchema().type(), fieldName));
          if (unionMember.getSchema().type().equals(AvroType.NULL)) {
            codeBlockBuilder.addStatement("$L = null", fieldName);
          }
          codeBlockBuilder.endControlFlow();
        }
        codeBlockBuilder.beginControlFlow("else")
            .addStatement("throw new $T($S)", IndexOutOfBoundsException.class, "Union IndexOutOfBounds")
            .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
      case RECORD:
        TypeName className = getTypeName(fieldSchema, fieldType);

        codeBlockBuilder.beginControlFlow("if($L == null)", fieldName)
        .addStatement("$L = new $T()", fieldName, className)
        .endControlFlow()
        .addStatement("$L.customDecode(in)", fieldName);

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
    }
    return SINGLE_DOLLAR_SIGN_REGEX.matcher(serializedCodeBlock).replaceAll("\\$\\$");
  }


  private boolean hasCustomCoders(AvroRecordSchema recordSchema) {
    return true;
  }

  private void addCustomEncodeMethod(MethodSpec.Builder customEncodeBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) {
    for(AvroSchemaField field : recordSchema.getFields()) {
      String escapedFieldName = getFieldNameWithSuffix(field);
      customEncodeBuilder.addStatement(getSerializedCustomEncodeBlock(config, field.getSchemaOrRef().getSchema(),
          field.getSchemaOrRef().getSchema().type(), replaceSingleDollarSignWithDouble(escapedFieldName)));
    }
  }

  private String getSerializedCustomEncodeBlock(SpecificRecordGenerationConfig config,
      AvroSchema fieldSchema, AvroType fieldType, String fieldName) {
    String serializedCodeBlock = "";
    CodeBlock.Builder codeBlockBuilder  = CodeBlock.builder();
    switch (fieldType) {

      case NULL:
        serializedCodeBlock = "out.writeNull()";
        break;
      case BOOLEAN:
        serializedCodeBlock = String.format("out.writeBoolean(%s)", fieldName);
        break;
      case INT:
        serializedCodeBlock = String.format("out.writeInt(%s)", fieldName);
        break;
      case LONG:
        serializedCodeBlock = String.format("out.writeLong(%s)", fieldName);
        break;
      case FLOAT:
        serializedCodeBlock = String.format("out.writeFloat(%s)", fieldName);
        break;
      case STRING:
        serializedCodeBlock = String.format("out.writeString(%s)", fieldName);
        break;
      case DOUBLE:
        serializedCodeBlock = String.format("out.writeDouble(%s)", fieldName);
        break;
      case BYTES:
        serializedCodeBlock = String.format("out.writeBytes(%s)", fieldName);
        break;
      case ENUM:
        serializedCodeBlock = String.format("out.writeEnum(%s.ordinal())", fieldName);
        break;
      case FIXED:
        serializedCodeBlock = String.format("out.writeFixed(%s.bytes(), 0, %s)", fieldName,
            ((AvroFixedSchema) fieldSchema).getSize());
        break;
      case ARRAY:
        String lengthVarName = getSizeVarName();
        String actualSizeVarName = getActualSizeVarName();
        AvroType arrayItemAvroType = ((AvroArraySchema) fieldSchema).getValueSchema().type();
        Class<?> arrayItemClass = getJavaClassForAvroTypeIfApplicable(arrayItemAvroType);
        TypeName arrayItemTypeName = getTypeName(((AvroArraySchema) fieldSchema).getValueSchema(), arrayItemAvroType);
        if(arrayItemClass != null) {
          fullyQualifiedClassesInRecord.add(arrayItemClass.getName());
        } else {
          fullyQualifiedClassesInRecord.add(arrayItemTypeName.toString());
        }

        codeBlockBuilder.addStatement("long $L = $L.size()", lengthVarName, fieldName)
            .addStatement("out.writeArrayStart()")
            .addStatement("out.setItemCount($L)", lengthVarName)
            .addStatement("long $L = 0", actualSizeVarName)
            .beginControlFlow("for ($T $L: $L)", arrayItemClass != null ? arrayItemClass : arrayItemTypeName,
                getElementVarName(), fieldName)
            .addStatement("$L++", actualSizeVarName)
            .addStatement("out.startItem()")
            .addStatement(getSerializedCustomEncodeBlock(config, ((AvroArraySchema) fieldSchema).getValueSchema(),
                arrayItemAvroType, getElementVarName()))
            .endControlFlow();

        codeBlockBuilder
            .addStatement("out.writeArrayEnd()")
            .beginControlFlow("if ($L != $L)", actualSizeVarName, lengthVarName)
            .addStatement("throw new $T(\"Array-size written was \" + $L + \", but element count was \" + $L + \".\")",
            ConcurrentModificationException.class, lengthVarName, actualSizeVarName);
        codeBlockBuilder.endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        sizeValCounter++;
        break;
      case MAP:
        lengthVarName = getSizeVarName();
        actualSizeVarName = getActualSizeVarName();
        String elementVarName = getElementVarName();
        String valueVarName = getValueVarName();

        codeBlockBuilder
            .addStatement("long $L = $L.size()", lengthVarName, fieldName)
            .addStatement("out.writeMapStart()")
            .addStatement("long $L = 0", actualSizeVarName);

        AvroType mapItemAvroType = ((AvroMapSchema) fieldSchema).getValueSchema().type();
        Class<?> mapItemClass = getJavaClassForAvroTypeIfApplicable(mapItemAvroType);

        if (mapItemClass != null) {
          codeBlockBuilder.beginControlFlow(
              "for (java.util.Map.Entry<java.lang.CharSequence, $T> $L: $L.entrySet())", mapItemClass,
              elementVarName, fieldName);
        } else {
          TypeName mapItemClassName = getTypeName(((AvroMapSchema) fieldSchema).getValueSchema(), mapItemAvroType);
          codeBlockBuilder.beginControlFlow(
              "for (java.util.Map.Entry<java.lang.CharSequence, $T> $L: $L.entrySet())", mapItemClassName,
              elementVarName, fieldName);
        }
        codeBlockBuilder
            .addStatement("$L++", actualSizeVarName)
            .addStatement("out.startItem()")
            .addStatement("out.writeString($L.getKey())", elementVarName);


        if (mapItemClass != null) {
          codeBlockBuilder.addStatement("$T $L = $L.getValue()", mapItemClass, valueVarName, elementVarName);
        } else {
          TypeName mapItemClassName = getTypeName(((AvroMapSchema) fieldSchema).getValueSchema(), mapItemAvroType);
          codeBlockBuilder.addStatement("$T $L = $L.getValue()", mapItemClassName, valueVarName, elementVarName);
        }
        codeBlockBuilder.addStatement(
            getSerializedCustomEncodeBlock(config, ((AvroMapSchema) fieldSchema).getValueSchema(), mapItemAvroType,
                valueVarName))
            .endControlFlow()
            .addStatement("out.writeMapEnd()")
            .beginControlFlow("if ($L != $L)", actualSizeVarName, lengthVarName)
            .addStatement("throw new $T(\"Map-size written was \" + $L + \", but element count was \" + $L + \".\")",
            ConcurrentModificationException.class, lengthVarName, actualSizeVarName)
            .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        sizeValCounter++;
        break;
      case UNION:
        int numberOfUnionMembers = ((AvroUnionSchema) fieldSchema).getTypes().size();

        for (int i = 0; i < numberOfUnionMembers; i++) {
          AvroSchema unionMemberSchema = ((AvroUnionSchema) fieldSchema).getTypes().get(i).getSchema();
          Class<?> unionMemberType = getJavaClassForAvroTypeIfApplicable(unionMemberSchema.type());
          TypeName unionMemberTypeName = getTypeName(unionMemberSchema, unionMemberSchema.type());

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
              .addStatement(getSerializedCustomEncodeBlock(config, unionMemberSchema, unionMemberSchema.type(),
                   fieldName));
        }
        codeBlockBuilder.endControlFlow()
            .beginControlFlow("else")
            .addStatement("throw new $T($S)", IllegalArgumentException.class, "Value does not match any union member")
            .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
      case RECORD:
        serializedCodeBlock = String.format("%s.customEncode(out)", fieldName);
        break;
    }
    return SINGLE_DOLLAR_SIGN_REGEX.matcher(serializedCodeBlock).replaceAll("\\$\\$");
  }

  private String getKeyVarName() {
    return "k" + sizeValCounter;
  }

  private String getValueVarName() {
    return "v" + sizeValCounter;
  }

  private String getElementVarName() {
    return "e" + sizeValCounter;
  }

  private String getSizeVarName() {
    return "size" + sizeValCounter;
  }

  private String getActualSizeVarName() {
    return "actualSize" + sizeValCounter;
  }

  private String getArrayVarName() {
    return "a" + sizeValCounter;
  }

  private String getGArrayVarName() {
    return "ga" + sizeValCounter;
  }

  private String getMapVarName() {
    return "m" + sizeValCounter;
  }

  /***
   *
   * @param avroType
   * @return
   *      Returns Java class for matching types
   *      Returns null if TypeName should be used instead
   */
  @Nullable
  private Class<?> getJavaClassForAvroTypeIfApplicable(AvroType avroType) {
    Class<?> cls;
    switch (avroType) {
      case NULL:
        cls = java.lang.Void.class;
        break;
      case ENUM:
        cls = java.lang.Enum.class;
        break;
      case BOOLEAN:
        cls = java.lang.Boolean.class;
        break;
      case INT:
        cls = java.lang.Integer.class;
        break;
      case FLOAT:
        cls = java.lang.Float.class;
        break;
      case LONG:
        cls = java.lang.Long.class;
        break;
      case DOUBLE:
        cls = java.lang.Double.class;
        break;
      case STRING:
        cls = java.lang.CharSequence.class;
        break;
      case BYTES:
        cls = ByteBuffer.class;
        break;
      default:
        cls = null;
    }
    return cls;
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
          getFieldClass(field.getSchemaOrRef().getSchema().type(), config.getDefaultFieldStringRepresentation());
      if(fieldClass != null) {
        fullyQualifiedClassesInRecord.add(StringConverterUtil.class.getName());
        if(field.getSchemaOrRef().getSchema().type() == AvroType.STRING) {
          switchBuilder.addStatement("case $L: this.$L = new $T(value).get$L(); break", fieldIndex++, escapedFieldName,
              StringConverterUtil.class, fieldClass.getSimpleName());
        } else {
          switchBuilder.addStatement("case $L: this.$L = ($T) value; break", fieldIndex++, escapedFieldName, fieldClass);
        }
      } else {
        switchBuilder.addStatement("case $L: this.$L = ($T) value; break", fieldIndex++, escapedFieldName,
            getTypeName(field.getSchemaOrRef().getSchema(), field.getSchemaOrRef().getSchema().type()));
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

      if(field.getSchemaOrRef().getSchema().type() == AvroType.STRING) {
        Class<?> fieldClass = getFieldClass(AvroType.STRING, config.getDefaultMethodStringRepresentation());
        switchBuilder.addStatement("case $L: return new $T($L).get$L()", fieldIndex++, StringConverterUtil.class,
            escapedFieldName, fieldClass.getSimpleName());
      } else {
        switchBuilder.addStatement("case $L: return $L", fieldIndex++, escapedFieldName);
      }
    }
    switchBuilder.addStatement("default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\")")
        .endControlFlow();

    classBuilder.addMethod(methodSpecBuilder.addCode(switchBuilder.build()).build());
  }

  private void addDefaultFullyQualifiedClassesForSpecificRecord(TypeSpec.Builder classBuilder,
      AvroRecordSchema recordSchema) {

    List<String> fieldNamesInRecord =
        recordSchema.getFields().stream().map(AvroSchemaField::getName).collect(Collectors.toList());

    for(String classToQualify: fullyQualifiedClassesInRecord) {
      String[] splitClassName = classToQualify.split("\\.");
      if(!fieldNamesInRecord.contains(splitClassName[0])){
        classBuilder.alwaysQualify(splitClassName[splitClassName.length-1]);
      }
    }

    for(TypeName classNameToQualify: fullyQualifiedClassNamesInRecord) {

      if(!fieldNamesInRecord.contains(classNameToQualify.toString().split("\\.")[0])){
        String[] fullyQualifiedNameArray = classNameToQualify.toString().split("\\.");
        classBuilder.alwaysQualify(fullyQualifiedNameArray[fullyQualifiedNameArray.length-1]);
      }
    }
  }

  private void addFullyQualified(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    if(field.getSchemaOrRef().getSchema() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getSchema().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        fullyQualifiedClassesInRecord.add(fieldClass.getName());
      } else {
        fullyQualifiedClassNamesInRecord.add(
            getTypeName(field.getSchemaOrRef().getSchema(), field.getSchemaOrRef().getSchema().type()));
      }
    }
  }

  private MethodSpec getSetterMethodSpec(AvroSchemaField field, SpecificRecordGenerationConfig config) {

    String escapedFieldName = getFieldNameWithSuffix(field);

    MethodSpec.Builder methodSpecBuilder = MethodSpec
        .methodBuilder(getMethodNameForFieldWithPrefix("set", escapedFieldName))
        .addStatement("this.$1L = $1L", escapedFieldName)
        .addModifiers(Modifier.PUBLIC);

    if(field.getSchemaOrRef().getSchema() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getSchema().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        methodSpecBuilder.addParameter(fieldClass, escapedFieldName)
            .addModifiers(Modifier.PUBLIC);
      } else {
        TypeName className = getTypeName(field.getSchemaOrRef().getSchema(), field.getSchemaOrRef().getSchema().type());
        methodSpecBuilder.addParameter(className, escapedFieldName);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      methodSpecBuilder.addParameter(className, escapedFieldName);

    }

    return methodSpecBuilder.build();
  }

  private MethodSpec getGetterMethodSpec(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    AvroType fieldType = field.getSchemaOrRef().getSchema().type();
    String escapedFieldName = getFieldNameWithSuffix(field);
    MethodSpec.Builder methodSpecBuilder = MethodSpec
        .methodBuilder(getMethodNameForFieldWithPrefix("get", field.getName())).addModifiers(Modifier.PUBLIC);

    if(field.getSchemaOrRef().getSchema() != null) {
      Class<?> fieldClass = getFieldClass(fieldType, config.getDefaultMethodStringRepresentation());
      if (fieldClass != null) {
        methodSpecBuilder.returns(fieldClass);
      } else {
        TypeName className = getTypeName(field.getSchemaOrRef().getSchema(), fieldType);
        methodSpecBuilder.returns(className);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      methodSpecBuilder.returns(className);
    }
    // if fieldRepresentation != methodRepresentation for String field
    if (AvroType.STRING.equals(fieldType)
        && config.getDefaultFieldStringRepresentation() != config.getDefaultMethodStringRepresentation()) {
      switch (config.getDefaultMethodStringRepresentation()) {
        case STRING:
          methodSpecBuilder.addStatement("return String.valueOf(this.$L)", escapedFieldName);
          break;

        case CHAR_SEQUENCE:
          methodSpecBuilder.addStatement("return this.$L", escapedFieldName);
          break;

        case UTF8:
          if (AvroJavaStringRepresentation.STRING.equals(config.getDefaultFieldStringRepresentation())) {
            methodSpecBuilder.addStatement("return new Utf8(this.$L)", escapedFieldName);
          } else if (AvroJavaStringRepresentation.CHAR_SEQUENCE.equals(config.getDefaultFieldStringRepresentation())) {
            methodSpecBuilder.addStatement("return new Utf8(String.valueOf(this.$L))", escapedFieldName);
          }
      }
    } else {
      methodSpecBuilder.addStatement("return this.$L", escapedFieldName);
    }

    return methodSpecBuilder.build();
  }

  private FieldSpec.Builder getFieldSpecBuilder(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    FieldSpec.Builder fieldSpecBuilder;
    String escapedFieldName = getFieldNameWithSuffix(field);
    if(field.getSchemaOrRef().getSchema() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getSchema().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        fieldSpecBuilder = FieldSpec.builder(fieldClass, escapedFieldName);
      } else {
        TypeName className = getTypeName(field.getSchemaOrRef().getSchema(), field.getSchemaOrRef().getSchema().type());
        fieldSpecBuilder = FieldSpec.builder(className, escapedFieldName);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      fieldSpecBuilder = FieldSpec.builder(className, escapedFieldName);
    }
    return fieldSpecBuilder;
  }

  private ParameterSpec getParameterSpecForField(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    ParameterSpec.Builder parameterSpecBuilder = null;
    String escapedFieldName = getFieldNameWithSuffix(field);
    if(field.getSchemaOrRef().getSchema() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getSchema().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
          parameterSpecBuilder = ParameterSpec.builder(fieldClass, escapedFieldName);
      } else {
        TypeName typeName = getTypeName(field.getSchemaOrRef().getSchema(), field.getSchemaOrRef().getSchema().type());
        parameterSpecBuilder = ParameterSpec.builder(typeName, escapedFieldName);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      parameterSpecBuilder = ParameterSpec.builder(className, escapedFieldName);
    }
    return parameterSpecBuilder.build();
  }

  private TypeName getTypeName(AvroSchema fieldSchema, AvroType avroType) {
    TypeName className = ClassName.OBJECT;
    switch (avroType) {
      case NULL:
        className = ClassName.get(Void.class);
        break;
      case ENUM:
        AvroName enumName = ((AvroEnumSchema) fieldSchema).getName();
        className = ClassName.get(enumName.getNamespace(), enumName.getSimpleName());
        break;
      case FIXED:
        AvroName fixedName = ((AvroFixedSchema) fieldSchema).getName();
        className = ClassName.get(fixedName.getNamespace(), fixedName.getSimpleName());
        break;
      case RECORD:
        AvroName recordName = ((AvroRecordSchema) fieldSchema).getName();
        className = ClassName.get(recordName.getNamespace(), recordName.getSimpleName());
        break;
      case UNION:
        // if union is [type] or [null, type], className can be type. Else Object.class
        AvroUnionSchema unionSchema = (AvroUnionSchema) fieldSchema;

        if(isSingleTypeNullableUnionSchema(unionSchema)) {
          List<SchemaOrRef> branches = unionSchema.getTypes();
          SchemaOrRef unionMemberSchemaOrRef = (branches.size() == 1) ? branches.get(0)
              : (branches.get(0).getSchema().type().equals(AvroType.NULL) ? branches.get(1)
                  : branches.get(0));

          AvroSchema branchSchema = unionMemberSchemaOrRef.getSchema();
          AvroType branchSchemaType = branchSchema.type();
          Class<?> simpleClass = getJavaClassForAvroTypeIfApplicable(branchSchemaType);
          if (simpleClass != null) {
            className = ClassName.get(simpleClass);
          } else {
            className = getTypeName(branchSchema, branchSchemaType);
          }
        } //otherwise Object
        break;
      case ARRAY:
        AvroArraySchema arraySchema = ((AvroArraySchema) fieldSchema);
        Class<?> valueClass = getJavaClassForAvroTypeIfApplicable(arraySchema.getValueSchema().type());
        if (valueClass == null) {
          className = ParameterizedTypeName.get(ClassName.get(List.class),
              getTypeName(arraySchema.getValueSchema(), arraySchema.getValueSchema().type()));
        } else {
          className = ParameterizedTypeName.get(List.class, valueClass);
        }
        break;
      case MAP:
        AvroMapSchema mapSchema = ((AvroMapSchema) fieldSchema);
        Class<?> mapValueClass = getJavaClassForAvroTypeIfApplicable(mapSchema.getValueSchema().type());
        //complex map is allowed
        if(mapValueClass == null) {
          className = ParameterizedTypeName.get(ClassName.get(Map.class), TypeName.get(CharSequence.class),
              getTypeName(mapSchema.getValueSchema(), mapSchema.getValueSchema().type()));
        } else {
          className = ParameterizedTypeName.get(Map.class, CharSequence.class, mapValueClass);
        }
        break;
      default:
        break;
    }
    return className;
  }

  private boolean isSingleTypeNullableUnionSchema(AvroUnionSchema unionSchema) {
    if(unionSchema.getTypes().size() == 1) return true;
    if(unionSchema.getTypes().size() == 2) {
      for(SchemaOrRef unionMember : unionSchema.getTypes()) {
        if(AvroType.NULL.equals(unionMember.getSchema().type())) {
          return true;
        }
      }
    }
    return false;
  }

  private Class<?> getFieldClass(AvroType fieldType, AvroJavaStringRepresentation defaultStringRepresentation) {
    Class<?> fieldClass = null;
    switch (fieldType) {
      case NULL:
        fieldClass = Void.class;
        break;
      case BOOLEAN:
         fieldClass = Boolean.class;
         break;
      case INT:
        fieldClass =  Integer.class;
        break;
      case LONG:
        fieldClass =  Long.class;
        break;
      case FLOAT:
        fieldClass =  Float.class;
        break;
      case STRING:
        switch (defaultStringRepresentation) {
          case STRING:
            fieldClass = String.class;
            break;
          case UTF8:
            fieldClass = Utf8.class;
            break;
          case CHAR_SEQUENCE:
            fieldClass = CharSequence.class;
            break;
        }
        break;
      case DOUBLE:
        fieldClass = Double.class;
        break;
      case BYTES:
        fieldClass = ByteBuffer.class;
        break;

    }
    return fieldClass;
  }

  private void addAndInitializeSizeFieldToClass(TypeSpec.Builder classBuilder, AvroFixedSchema fixedSchema)
      throws ClassNotFoundException {
    classBuilder.addAnnotation(AnnotationSpec.builder(CLASSNAME_FIXED_SIZE)
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
    ClassName avroSchemaType = CLASSNAME_SCHEMA;
    classBuilder.alwaysQualify(avroSchemaType.simpleName()); //no import statements

    //get fully-inlined single-line avsc from schema
    AvscSchemaWriter avscWriter = new AvscSchemaWriter();
    String avsc = avscWriter.writeSingle(classSchema).getContents();

    //JVM spec spec says string literals cant be over 65535 bytes in size (this isnt simply the
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
