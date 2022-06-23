/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.CompatibleSpecificRecordBuilderBase;
import com.linkedin.avroutil1.compatibility.HelperConsts;
import com.linkedin.avroutil1.compatibility.SourceCodeUtils;
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
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.tools.JavaFileObject;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;



/**
 * generates java classes out of avro schemas.
 */
public class SpecificRecordClassGenerator {

  private static final String AVRO_GEN_COMMENT = "GENERATED CODE by avro-util";
  private int sizeValCounter = 0;


  HashSet<TypeName> fullyQualifiedClassNamesInRecord = new HashSet<>();
  HashSet<Class<? extends Object>> fullyQualifiedClassesInRecord = new HashSet<>(Arrays.asList(
      IOException.class,
      Exception.class,
      ObjectInput.class,
      ObjectOutput.class,
      String.class,
      DatumReader.class,
      DatumWriter.class,
      SpecificData.class,
      SpecificDatumReader.class,
      SpecificDatumWriter.class,
      SpecificRecord.class,
      SpecificRecordBase.class,
      Object.class,
      Encoder.class,
      ConcurrentModificationException.class,
      ResolvingDecoder.class,
      IllegalArgumentException.class,
      IndexOutOfBoundsException.class,
      HashMap.class,
      CompatibleSpecificRecordBuilderBase.class
  ));

  public JavaFileObject generateSpecificRecordClass(AvroNamedSchema topLevelSchema, SpecificRecordGenerationConfig config)
      throws ClassNotFoundException {
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


  protected JavaFileObject generateSpecificFixed(AvroFixedSchema fixedSchema, SpecificRecordGenerationConfig config) {
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
    //public class
    TypeSpec.Builder classBuilder = TypeSpec.classBuilder(recordSchema.getSimpleName());
    classBuilder.addModifiers(Modifier.PUBLIC);

    //extends
    classBuilder.addSuperinterface(SpecificRecord.class);

    // implements
    classBuilder.superclass(SpecificRecordBase.class);

    //add class-level doc from schema doc
    //file-level (top of file) comment is added to the file object later
    String doc = recordSchema.getDoc();
    if (doc != null && !doc.isEmpty()) {
      classBuilder.addJavadoc(doc);
    }

    if(config != null) {
      if(config.getMinimumSupportedAvroVersion().laterThan(AvroVersion.AVRO_1_7)) {
        // MODEL$ as SpecificData()
        classBuilder.addField(
            FieldSpec.builder(ClassName.get("org.apache.avro.specific", "SpecificData"), "MODEL$", Modifier.PRIVATE,
                Modifier.STATIC)
                .initializer(CodeBlock.of("new $T()", ClassName.get("org.apache.avro.specific", "SpecificData")))
                .build());
      } else {
        classBuilder.addField(
            FieldSpec.builder(ClassName.get("org.apache.avro.specific", "SpecificData"), "MODEL$", Modifier.PRIVATE,
                Modifier.STATIC)
                .initializer(CodeBlock.of("$T.get()", ClassName.get("org.apache.avro.specific", "SpecificData")))
                .build());
      }

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
        .returns(Schema.class)
        .addStatement("return $L", "SCHEMA$")
        .build());


    if(config.getMinimumSupportedAvroVersion().laterThan(AvroVersion.AVRO_1_7)) {
      // read external
      classBuilder.addField(
          FieldSpec.builder(DatumReader.class, "READER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
              .initializer(CodeBlock.of("new $T($L)", SpecificDatumReader.class, "SCHEMA$"))
              .build());

      MethodSpec.Builder readExternalBuilder = MethodSpec.methodBuilder("readExternal")
          .addException(IOException.class)
          .addParameter(java.io.ObjectInput.class, "in")
          .addModifiers(Modifier.PUBLIC)
          .addCode(CodeBlock.builder().addStatement("$L.read(this, $T.getDecoder(in))", "READER$", SpecificData.class).build());

      // write external
      classBuilder.addField(
          FieldSpec.builder(DatumWriter.class, "WRITER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
              .initializer(CodeBlock.of("new $T($L)", SpecificDatumWriter.class, "SCHEMA$"))
              .build());

      MethodSpec.Builder writeExternalBuilder = MethodSpec
          .methodBuilder("writeExternal")
          .addException(IOException.class)
          .addParameter(java.io.ObjectOutput.class, "out")
          .addModifiers(Modifier.PUBLIC)
          .addCode(CodeBlock
              .builder()
              .addStatement("$L.write(this, $T.getEncoder(out))", "WRITER$", SpecificData.class)
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
          addFullyQualified(field, config);
          allArgsConstructorBuilder.addParameter(getParameterSpecForField(field, config))
              .addStatement("this.$L = $L", field.getName(), field.getName());
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
    addGetByIndexMethod(classBuilder, recordSchema);

    //Add put method by index
    addPutByIndexMethod(classBuilder, recordSchema, config);

    //hasCustomCoders
    if(hasCustomCoders(recordSchema)){
      classBuilder.addMethod(
          MethodSpec.methodBuilder("hasCustomCoders")
              .addModifiers(Modifier.PROTECTED)
              .returns(boolean.class)
              .addStatement("return true")
              .build());

      // customEncode
      MethodSpec.Builder customEncodeBuilder = MethodSpec
          .methodBuilder("customEncode")
          .addParameter(Encoder.class, "out")
          .addException(IOException.class)
          .addModifiers(Modifier.PUBLIC);
      addCustomEncodeMethod(customEncodeBuilder, recordSchema, config);
      classBuilder.addMethod(customEncodeBuilder.build());

      //customDecode
      MethodSpec.Builder customDecodeBuilder = MethodSpec
          .methodBuilder("customDecode")
          .addParameter(ResolvingDecoder.class, "in")
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
    // All private fields
    for (AvroSchemaField field : recordSchema.getFields()) {
      FieldSpec.Builder fieldBuilder;
      AvroType fieldAvroType = field.getSchemaOrRef().getDecl().type();
      Class<?> fieldClass = avroTypeToJavaClass(fieldAvroType);
      TypeName fieldType = getTypeName(field.getSchema(), fieldAvroType);
      if (fieldClass != null) {
        fieldBuilder = FieldSpec.builder(fieldClass, field.getName(), Modifier.PRIVATE);
        buildMethodCodeBlockBuilder.addStatement(
            "record.$1L = fieldSetFlags()[$2L] ? this.$1L : ($3T) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
            field.getName(), fieldIndex, fieldClass);
      } else {
        fieldBuilder = FieldSpec.builder(fieldType, field.getName(), Modifier.PRIVATE);
        if(!AvroType.RECORD.equals(fieldAvroType)) {
          buildMethodCodeBlockBuilder.addStatement(
              "record.$1L = fieldSetFlags()[$2L] ? this.$1L : ($3L) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
              field.getName(), fieldIndex, fieldType);
        } else {
          buildMethodCodeBlockBuilder
              .beginControlFlow("if ($L != null)", field.getName()+"Builder")
              .beginControlFlow("try")
              .addStatement("record.$1L = this.$1LBuilder.build()", field.getName())
              .endControlFlow()
              .beginControlFlow("catch (org.apache.avro.AvroMissingFieldException  e)")
              .addStatement("e.addParentField(record.getSchema().getField($S))", field.getName())
              .addStatement("throw e")
              .endControlFlow()
              .endControlFlow()
              .beginControlFlow("else")
              .addStatement(
              "record.$1L = fieldSetFlags()[$2L] ? this.$1L : ($3L) com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper.getSpecificDefaultValue(fields()[$2L])",
              field.getName(), fieldIndex, fieldType)
          .endControlFlow();
        }
      }
      if (field.hasDoc()) {
        fieldBuilder.addJavadoc(field.getDoc());
      }
      recordBuilder.addField(fieldBuilder.build());

      otherBuilderConstructorFromRecordBlockBuilder.beginControlFlow("if (isValidValue(fields()[$L], other.$L))", fieldIndex,
          field.getName())
          .addStatement("this.$1L = deepCopyField(other.$1L, fields()[$2L].schema(), $3S)", field.getName(), fieldIndex,
              config.getDefaultFieldStringRepresentation().getJsonValue())
          .addStatement("fieldSetFlags()[$L] = true", fieldIndex)
          .endControlFlow();

      otherBuilderConstructorFromOtherBuilderBlockBuilder.beginControlFlow("if (isValidValue(fields()[$L], other.$L))", fieldIndex,
          field.getName())
          .addStatement("this.$1L = deepCopyField(other.$1L, fields()[$2L].schema(), $3S)", field.getName(), fieldIndex,
              config.getDefaultFieldStringRepresentation().getJsonValue())
          .addStatement("fieldSetFlags()[$1L] = other.fieldSetFlags()[$1L]", fieldIndex)
          .endControlFlow();

      if (AvroType.RECORD.equals(fieldAvroType)) {
        recordBuilder.addField(
            FieldSpec.builder(ClassName.get(((AvroRecordSchema) field.getSchema()).getFullName(), "Builder"),
                field.getName() + "Builder").build());

        populateRecordBuilderAccessor(accessorMethodSpecs, field.getName(), recordSchema.getFullName(), field);

        otherBuilderConstructorFromRecordBlockBuilder.addStatement("this.$L = null", field.getName()+"Builder");
        otherBuilderConstructorFromOtherBuilderBlockBuilder.beginControlFlow("if (other.$L())",
            getMethodNameForFieldWithPrefix("has", field.getName() + "Builder"))
            .addStatement("this.$L = $L.newBuilder(other.$L())", field.getName() + "Builder",
                ((AvroRecordSchema) field.getSchema()).getFullName(),
                getMethodNameForFieldWithPrefix("get", field.getName() + "Builder"))
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
        .endControlFlow()
        .beginControlFlow("catch (org.apache.avro.AvroMissingFieldException e)")
        .addStatement("throw e")
        .endControlFlow()
        .beginControlFlow("catch ($T e)", Exception.class)
        .addStatement("throw new org.apache.avro.AvroRuntimeException(e)")
        .endControlFlow();

    recordBuilder.addMethod(
        MethodSpec.methodBuilder("build")
            .addModifiers(Modifier.PUBLIC)
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
        .addStatement("$L($L.newBuilder($L))", getMethodNameForFieldWithPrefix("set", builderName),
            fieldParentNamespace, fieldName)
        .endControlFlow()
        .beginControlFlow("else")
        .addStatement("$L($L.newBuilder())", getMethodNameForFieldWithPrefix("set", builderName),
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

    //Getter
    MethodSpec.Builder getMethodBuilder =
        MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("get", field.getName()))
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc("Gets the value of the '$L' field.$L" + "@return The value.", field.getName(),
                getFieldJavaDoc(field))
        .addStatement("return $L", field.getName());
    if(fieldClass != null) {
      getMethodBuilder.returns(fieldClass);
    } else {
      getMethodBuilder.returns(fieldType);
    }

    //Setter
    MethodSpec.Builder setMethodBuilder =
        MethodSpec.methodBuilder(getMethodNameForFieldWithPrefix("set", field.getName()))
            .addModifiers(Modifier.PUBLIC)
            .addJavadoc(
                "Sets the value of the '$L' field.$L" + "@param value The value of '$L'.\n" + "@return This Builder.",
                field.getName(), getFieldJavaDoc(field), field.getName())
            .addStatement("validate(fields()[$L], value)", fieldIndex)
            .addStatement("this.$L = value", field.getName())
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
            .addStatement("$L = null", field.getName())
            .addStatement("fieldSetFlags()[$L] = false", fieldIndex)
            .addStatement("return this")
            .returns(ClassName.get(parentClass, "Builder"));

    accessorMethodSpecs.add(getMethodBuilder.build());
    accessorMethodSpecs.add(setMethodBuilder.build());
    accessorMethodSpecs.add(hasMethodBuilder.build());
    accessorMethodSpecs.add(clearMethodBuilder.build());
  }

  private String getFieldJavaDoc(AvroSchemaField field) {
    return (field.hasDoc() ? "\n" + field.getDoc() + "\n" : "\n");
  }

  private String getMethodNameForFieldWithPrefix(String prefix, String fieldName) {
    if (fieldName.length() < 1) {
      throw new IllegalArgumentException("FieldName must be longer than 1");
    }
    return prefix + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);

  }

  private void addCustomDecodeMethod(MethodSpec.Builder customDecodeBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) {
    // reset var counter
    sizeValCounter = 0;
    customDecodeBuilder.addStatement("org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff()")
        .beginControlFlow("if (fieldOrder == null)");
    for(AvroSchemaField field : recordSchema.getFields()) {
      customDecodeBuilder.addStatement(getSerializedCustomDecodeBlock(config, field.getSchemaOrRef().getDecl(),
          field.getSchemaOrRef().getDecl().type(), field.getName()));
    }
    // reset var counter
    sizeValCounter = 0;
    int fieldIndex = 0;
    customDecodeBuilder.endControlFlow()
        .beginControlFlow("else")
        .beginControlFlow("for( int i = 0; i< $L; i++)", recordSchema.getFields().size())
        .beginControlFlow("switch(fieldOrder[i].pos())");
    for(AvroSchemaField field : recordSchema.getFields()) {
      customDecodeBuilder
          .addStatement(String.format("case %s: ",fieldIndex++)+ getSerializedCustomDecodeBlock(config,
              field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type(), field.getName()))
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
        AvroType arrayItemAvroType = ((AvroArraySchema) fieldSchema).getValueSchema().type();
        Class<?> arrayItemClass = avroTypeToJavaClass(arrayItemAvroType);

        codeBlockBuilder
            .addStatement("long $L = in.readArrayStart()", getSizeVarName())
            .addStatement("$T<$T> $L = $L", List.class, arrayItemClass, arrayVarName, fieldName)
            .beginControlFlow("if($L == null)", arrayVarName)
            .addStatement("$L = new org.apache.avro.specific.SpecificData.Array<$T>((int)$L, $L.getField(\"nullArrayField\").schema())",
                arrayVarName, arrayItemClass, getSizeVarName(), "SCHEMA$$")
            .addStatement("$L = $L", fieldName, arrayVarName)
            .endControlFlow()
            .beginControlFlow("else")
            .addStatement("$L.clear()", arrayVarName)
            .endControlFlow();

        codeBlockBuilder.addStatement(
            "org.apache.avro.specific.SpecificData.Array<$T> $L = ($L instanceof org.apache.avro.specific.SpecificData.Array ? (org.apache.avro.specific.SpecificData.Array<$T>)$L : null)",
            arrayItemClass, gArrayVarName, arrayVarName, arrayItemClass, arrayVarName);
        codeBlockBuilder.beginControlFlow("for (; 0 < $L; $L = in.arrayNext())", getSizeVarName(), getSizeVarName())
            .beginControlFlow("for(; $L != 0; $L--)", getSizeVarName(), getSizeVarName())
            .addStatement("$T $L = ($L != null ? $L.peek() : null)", arrayItemClass, getElementVarName(), gArrayVarName, gArrayVarName);

        codeBlockBuilder.addStatement(
            getSerializedCustomDecodeBlock(config, ((AvroArraySchema) fieldSchema).getValueSchema(), arrayItemAvroType, getElementVarName()));
        codeBlockBuilder.addStatement("$L.add($L)", arrayVarName, getElementVarName())
            .endControlFlow()
            .endControlFlow();

        serializedCodeBlock = codeBlockBuilder.build().toString();

        sizeValCounter++;

        break;
      case MAP:

        String mapVarName = getMapVarName();
        AvroType mapItemAvroType = ((AvroMapSchema) fieldSchema).getValueSchema().type();
        Class<?> mapItemClass = avroTypeToJavaClass(mapItemAvroType);
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
              getSerializedCustomDecodeBlock(config, fieldSchema, unionMember.getSchema().type(), fieldName));
          if (unionMember.getDecl().type().equals(AvroType.NULL)) {
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

        codeBlockBuilder.beginControlFlow("if(this.$L == null)", fieldName)
        .addStatement("this.$L = new $T()", fieldName, className)
        .endControlFlow()
        .addStatement("this.$L.customDecode(in)", fieldName);

        serializedCodeBlock = codeBlockBuilder.build().toString();
        break;
    }
    return serializedCodeBlock;
  }


  private boolean hasCustomCoders(AvroRecordSchema recordSchema) {
    return true;
  }

  private void addCustomEncodeMethod(MethodSpec.Builder customEncodeBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) {

    for(AvroSchemaField field : recordSchema.getFields()) {
      customEncodeBuilder.addStatement(getSerializedCustomEncodeBlock(config, field.getSchemaOrRef().getDecl(),
          field.getSchemaOrRef().getDecl().type(), field.getName()));
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
        Class<?> arrayItemClass = avroTypeToJavaClass(arrayItemAvroType);
        fullyQualifiedClassesInRecord.add(arrayItemClass);

        codeBlockBuilder.addStatement("long $L = $L.size()", lengthVarName, fieldName)
            .addStatement("out.writeArrayStart()")
            .addStatement("out.setItemCount($L)", lengthVarName)
            .addStatement("long $L = 0", actualSizeVarName)
            .beginControlFlow("for ($T $L: $L)", arrayItemClass, getElementVarName(), fieldName)
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
        Class<?> mapItemClass = avroTypeToJavaClass(mapItemAvroType);

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
          SchemaOrRef unionMember = ((AvroUnionSchema) fieldSchema).getTypes().get(i);
          if (i == 0) {
            if (unionMember.getDecl().type().equals(AvroType.NULL)) {
              codeBlockBuilder.beginControlFlow("if ($L == null) ", fieldName);
            } else {
              codeBlockBuilder.beginControlFlow("if ($L instanceof $T) ", fieldName,
                  avroTypeToJavaClass(unionMember.getSchema().type()));
            }
          } else {
            codeBlockBuilder.endControlFlow();
            if (unionMember.getDecl().type().equals(AvroType.NULL)) {
              codeBlockBuilder.beginControlFlow(" else if ($L == null) ", fieldName);
            } else {
              codeBlockBuilder.beginControlFlow(" else if ($L instanceof $T) ", fieldName,
                  avroTypeToJavaClass(unionMember.getSchema().type()));
            }
          }
          codeBlockBuilder.addStatement("out.writeIndex($L)", i)
              .addStatement(getSerializedCustomEncodeBlock(config, fieldSchema, unionMember.getSchema().type(),
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
    return serializedCodeBlock;
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

  private Class<?> avroTypeToJavaClass(AvroType avroType) {
    Class<?> cls = null;
    switch (avroType) {
      case NULL:
        cls = java.lang.Void.class;
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

      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultMethodStringRepresentation());
      if(fieldClass != null) {
        switchBuilder.addStatement("case $L: $L = ($T) value; break", fieldIndex++, field.getName(),
            fieldClass);
      } else {
        switchBuilder.addStatement("case $L: $L = ($T) value; break", fieldIndex++, field.getName(),
            getTypeName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type()));
      }
    }
    switchBuilder.addStatement("default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\")")
        .endControlFlow();

    classBuilder.addMethod(methodSpecBuilder.addCode(switchBuilder.build()).build());
  }

  private void addGetByIndexMethod(TypeSpec.Builder classBuilder, AvroRecordSchema recordSchema) {
    int fieldIndex = 0;
    MethodSpec.Builder methodSpecBuilder = MethodSpec.methodBuilder("get")
        .returns(Object.class)
        .addParameter(int.class, "field")
        .addAnnotation(Override.class)
        .addModifiers(Modifier.PUBLIC);
    CodeBlock.Builder switchBuilder = CodeBlock.builder();
    switchBuilder.beginControlFlow("switch (field)");
    for (AvroSchemaField field : recordSchema.getFields()) {
      switchBuilder.addStatement("case $L: return $L", fieldIndex++, field.getName());
    }
    switchBuilder.addStatement("default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\")")
        .endControlFlow();

    classBuilder.addMethod(methodSpecBuilder.addCode(switchBuilder.build()).build());
  }

  private void addDefaultFullyQualifiedClassesForSpecificRecord(TypeSpec.Builder classBuilder,
      AvroRecordSchema recordSchema) {

    List<String> fieldNamesInRecord =
        recordSchema.getFields().stream().map(AvroSchemaField::getName).collect(Collectors.toList());

    for(Class<?> classToQualify: fullyQualifiedClassesInRecord) {

      if(!fieldNamesInRecord.contains(classToQualify.getName().split("\\.")[0])){
        classBuilder.alwaysQualify(classToQualify.getSimpleName());
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
    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        fullyQualifiedClassesInRecord.add(fieldClass);
      } else {
        fullyQualifiedClassNamesInRecord.add(
            getTypeName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type()));
      }
    }
  }

  private MethodSpec getSetterMethodSpec(AvroSchemaField field, SpecificRecordGenerationConfig config) {

    MethodSpec.Builder methodSpecBuilder = MethodSpec
        .methodBuilder(getMethodNameForFieldWithPrefix("set", field.getName()))
        .addStatement("this.$1L = $1L", field.getName())
        .addModifiers(Modifier.PUBLIC);

    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        methodSpecBuilder.addParameter(fieldClass, field.getName())
            .addModifiers(Modifier.PUBLIC);
      } else {
        TypeName className = getTypeName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
        methodSpecBuilder.addParameter(className, field.getName());
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      methodSpecBuilder.addParameter(className, field.getName());

    }

    return methodSpecBuilder.build();
  }

  private MethodSpec getGetterMethodSpec(AvroSchemaField field, SpecificRecordGenerationConfig config) {

    MethodSpec.Builder methodSpecBuilder = MethodSpec
        .methodBuilder(getMethodNameForFieldWithPrefix("get", field.getName()))
        .addStatement("return this.$L", field.getName()).addModifiers(Modifier.PUBLIC);

    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultMethodStringRepresentation());
      if (fieldClass != null) {
        methodSpecBuilder.returns(fieldClass);
      } else {
        TypeName className = getTypeName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
        methodSpecBuilder.returns(className);
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      methodSpecBuilder.returns(className);
    }

    return methodSpecBuilder.build();
  }

  private FieldSpec.Builder getFieldSpecBuilder(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    FieldSpec.Builder fieldSpecBuilder = null;
    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        fieldSpecBuilder = FieldSpec.builder(fieldClass, field.getName());
      } else {
        TypeName className = getTypeName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
        fieldSpecBuilder = FieldSpec.builder(className, field.getName());
        System.out.println(field.getSchemaOrRef().getDecl().type());
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      fieldSpecBuilder = FieldSpec.builder(className, field.getName());
    }
    return fieldSpecBuilder;
  }

  private ParameterSpec getParameterSpecForField(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    ParameterSpec.Builder parameterSpecBuilder = null;
    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
          parameterSpecBuilder = ParameterSpec.builder(fieldClass, field.getName());
      } else {
        TypeName typeName = getTypeName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
        parameterSpecBuilder = ParameterSpec.builder(typeName, field.getName());
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      parameterSpecBuilder = ParameterSpec.builder(className, field.getName());
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
          SchemaOrRef unionMemberSchemaOrRef = (unionSchema.getTypes().size() == 1) ? unionSchema.getTypes().get(0)
              : (unionSchema.getTypes().get(0).getDecl().type().equals(AvroType.NULL) ? unionSchema.getTypes().get(1)
                  : unionSchema.getTypes().get(0));

          className = ClassName.get(avroTypeToJavaClass(unionMemberSchemaOrRef.getDecl().type()));
        }
        break;
      case ARRAY:
        AvroArraySchema arraySchema = ((AvroArraySchema) fieldSchema);
        Class<?> valueClass = avroTypeToJavaClass(arraySchema.getValueSchema().type());
        className = ParameterizedTypeName.get(List.class, valueClass);
        break;
      case MAP:
        AvroMapSchema mapSchema = ((AvroMapSchema) fieldSchema);
        Class<?> mapValueClass = avroTypeToJavaClass(mapSchema.getValueSchema().type());
        //complex map is allowed
        if(mapValueClass == null) {
          className = ParameterizedTypeName.get(ClassName.get(Map.class), TypeName.get(CharSequence.class),
              getTypeName(mapSchema.getValueSchema(), mapSchema.getValueSchema().type()));
        } else {
          className = ParameterizedTypeName.get(Map.class, mapValueClass);
        }
        break;

    }
    return className;
  }

  private boolean isSingleTypeNullableUnionSchema(AvroUnionSchema unionSchema) {
    if(unionSchema.getTypes().size() == 1) return true;
    if(unionSchema.getTypes().size()  == 2) {
      for(SchemaOrRef unionMember : unionSchema.getTypes()) {
        if(AvroType.NULL.equals(unionMember.getDecl().type())) {
          return true;
        }
      }
    }
    return false;
  }

  private Class<?> getFieldClass(AvroType fieldType, AvroJavaStringRepresentation defaultFieldStringRepresentation)
       {
    Class<?> fieldClass = null;
    switch (fieldType) {
      case NULL:
//        return Class.forName("java.lang.Null");
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
//        switch (defaultFieldStringRepresentation) {
//          case STRING:
//            fieldClass = String.class;
//            break;
//          case UTF8:
//            fieldClass = Utf8.class;
//            break;
//          case CHAR_SEQUENCE:
//            fieldClass = CharSequence.class;
//            break;
//        }

        fieldClass = CharSequence.class;
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

  private void addAndInitializeSizeFieldToClass(TypeSpec.Builder classBuilder, AvroFixedSchema fixedSchema) {
    classBuilder.addAnnotation(AnnotationSpec.builder(FixedSize.class)
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
    ClassName avroSchemaType = ClassName.get("org.apache.avro", "Schema");
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
