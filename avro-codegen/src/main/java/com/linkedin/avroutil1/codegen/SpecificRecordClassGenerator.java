/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avroutil1.compatibility.AvroVersion;
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
import com.squareup.javapoet.TypeSpec;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.tools.JavaFileObject;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;


/**
 * generates java classes out of avro schemas.
 */
public class SpecificRecordClassGenerator {

  private static final String AVRO_GEN_COMMENT = "GENERATED CODE by avro-util";
  private int sizeValCounter = 0;

  private static final List<Class<?>> CLASSES_WITH_SUBTYPE = Arrays.asList(Map.class, List.class);

  HashSet<ClassName> fullyQualifiedClassNamesInRecord = new HashSet<>();
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
      ConcurrentModificationException.class
  ));

  public JavaFileObject generateSpecificRecordClass(AvroNamedSchema topLevelSchema, SpecificRecordGenerationConfig config) {
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

  protected JavaFileObject generateSpecificRecord(AvroRecordSchema recordSchema, SpecificRecordGenerationConfig config) {
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

    // read external
    classBuilder.addField(
        FieldSpec.builder(DatumReader.class, "READER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(CodeBlock.of("new $T($L)", SpecificDatumReader.class, "SCHEMA$"))
            .build());

    classBuilder.addMethod(MethodSpec
        .methodBuilder("readExternal")
        .addException(IOException.class)
        .addParameter(java.io.ObjectInput.class, "in")
        .addModifiers(Modifier.PUBLIC)
        .addCode(CodeBlock
            .builder()
            .addStatement("$L.read(this, SpecificData.getDecoder(in))", "READER$")
            .build())
        .build());

    // write external
    classBuilder.addField(
        FieldSpec.builder(DatumWriter.class, "WRITER$", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer(CodeBlock.of("new $T($L)", SpecificDatumWriter.class, "SCHEMA$"))
            .build());

    classBuilder.addMethod(MethodSpec
        .methodBuilder("writeExternal")
        .addException(IOException.class)
        .addParameter(java.io.ObjectOutput.class, "out")
        .addModifiers(Modifier.PUBLIC)
        .addCode(CodeBlock
            .builder()
            .addStatement("$L.write(this, SpecificData.getEncoder(out))", "WRITER$")
            .build())
        .build());

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
      Modifier accessModifier = (config == null || config.hasPublicFields())? Modifier.PUBLIC : Modifier.PRIVATE;
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

    //add public final static SCHEMA$
    addSchema$ToGeneratedClass(classBuilder, recordSchema);


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

  private boolean hasCustomCoders(AvroRecordSchema recordSchema) {
//    for(AvroSchemaField field : recordSchema.getFields()) {
//      if(AvroType.UNION.equals(field.getSchemaOrRef().getDecl().type())) {
//        List<SchemaOrRef> unionComponents = ((AvroUnionSchema) field.getSchemaOrRef().getDecl()).getTypes();
//        if(unionComponents.size() > 2) {
//          return false;
//        }
//        if(unionComponents.get(0).getDecl().type().equals(AvroType.NULL)) {
//          return unionComponents.get(1).getDecl().type().isPrimitive();
//        } else if (unionComponents.get(1).getDecl().type().equals(AvroType.NULL)) {
//          return unionComponents.get(0).getDecl().type().isPrimitive();
//        }
//        return false;
//      }
//    }
    return true;
  }

  private void addCustomEncodeMethod(MethodSpec.Builder customEncodeBuilder, AvroRecordSchema recordSchema,
      SpecificRecordGenerationConfig config) {

    for(AvroSchemaField field : recordSchema.getFields()) {
      populateCustomEncode(customEncodeBuilder, config, field, field.getSchemaOrRef().getDecl().type(), field.getName());
    }
  }

  private void populateCustomEncode(MethodSpec.Builder customEncodeBuilder, SpecificRecordGenerationConfig config,
      AvroSchemaField field, AvroType fieldType, String fieldName) {

//    if(field.getSchemaOrRef().getDecl().type().isPrimitive()) {
//      populateCustomEncodePrimitive(customEncodeBuilder, field.getSchemaOrRef().getDecl().type(), field.getName());
//    }


    switch (fieldType) {
        // Array and Map are placeholders at this point. To be enhanced

      case NULL:
        customEncodeBuilder.addStatement("out.writeNull()");
        break;
      case BOOLEAN:
        customEncodeBuilder.addStatement("out.writeBoolean(this.$L)", fieldName);
        break;
      case INT:
        customEncodeBuilder.addStatement("out.writeInt(this.$L)", fieldName);
        break;
      case LONG:
        customEncodeBuilder.addStatement("out.writeLong(this.$L)", fieldName);
        break;
      case FLOAT:
        customEncodeBuilder.addStatement("out.writeFloat(this.$L)", fieldName);
        break;
      case STRING:
        customEncodeBuilder.addStatement("out.writeString(this.$L)", fieldName);
        break;
      case DOUBLE:
        customEncodeBuilder.addStatement("out.writeDouble(this.$L)", fieldName);
        break;
      case BYTES:
        customEncodeBuilder.addStatement("out.writeBytes(this.$L)", fieldName);
        break;
      case ENUM:
        customEncodeBuilder.addStatement("out.writeEnum(this.$L.ordinal())", fieldName);
        break;
      case FIXED:
        customEncodeBuilder.addStatement("out.writeFixed(this.$L.bytes(), 0, $L)", fieldName,
            ((AvroFixedSchema) field.getSchema()).getSize());
        break;
      case ARRAY:
        String lengthVarName = getLengthVarName();
        String actualSizeVarName = getActualSizeVarName();
        AvroType arrayItemAvroType = ((AvroArraySchema) field.getSchemaOrRef().getDecl()).getValueSchema().type();
        Class<?> arrayItemClass = avroTypeToJavaClass(arrayItemAvroType);
        fullyQualifiedClassesInRecord.add(arrayItemClass);

        customEncodeBuilder.addStatement("long $L = this.$L.size()", lengthVarName, fieldName);

        customEncodeBuilder.addStatement("out.writeArrayStart()");
        customEncodeBuilder.addStatement("out.setItemCount($L)", lengthVarName);
        customEncodeBuilder.addStatement("long $L = 0", actualSizeVarName);
        customEncodeBuilder
            .beginControlFlow("for ($T e0: this.$L)", arrayItemClass, field.getName())
            .addStatement("$L++", actualSizeVarName)
            .addStatement("out.startItem()");
        populateCustomEncode(customEncodeBuilder, config, field, arrayItemAvroType, field.getName());
        customEncodeBuilder.endControlFlow();

        customEncodeBuilder.addStatement("out.writeArrayEnd()");
        customEncodeBuilder.beginControlFlow("if ($L != $L)", actualSizeVarName, lengthVarName);
        customEncodeBuilder.addStatement("throw new $T(\"Array-size written was \" + $L + \", but element count was \" + $L + \".\")",
            ConcurrentModificationException.class, lengthVarName, actualSizeVarName);
        customEncodeBuilder.endControlFlow();
        sizeValCounter++;
        break;
      case MAP:
        lengthVarName = getLengthVarName();
        actualSizeVarName = getActualSizeVarName();
        String elementVarName = getElementVarName();
        String valueVarName = getValueVarName();
        customEncodeBuilder.addStatement("long $L = this.$L.size()", lengthVarName, field.getName());
        customEncodeBuilder.addStatement("out.writeMapStart()");
        customEncodeBuilder.addStatement("long $L = 0", actualSizeVarName);
        AvroType mapItemAvroType = ((AvroMapSchema) field.getSchemaOrRef().getDecl()).getValueSchema().type();
        Class<?> mapItemClass = avroTypeToJavaClass(mapItemAvroType);

        if (mapItemClass != null) {
          customEncodeBuilder.beginControlFlow(
              "for (java.util.Map.Entry<java.lang.CharSequence, $T> $L: this.$L.entrySet())", mapItemClass,
              elementVarName, field.getName());
        } else {
          ClassName mapItemClassName = getClassName(((AvroMapSchema) field.getSchemaOrRef().getDecl()).getValueSchema(), mapItemAvroType);
          customEncodeBuilder.beginControlFlow(
              "for (java.util.Map.Entry<java.lang.CharSequence, $T> $L: this.$L.entrySet())", mapItemClassName,
              elementVarName, field.getName());
        }
        customEncodeBuilder.addStatement("$L++", actualSizeVarName)
            .addStatement("out.startItem()");
        customEncodeBuilder.addStatement("out.writeString($L.getKey())", elementVarName);


        if (mapItemClass != null) {
          customEncodeBuilder.addStatement("$T v2 = $L.getValue()", mapItemClass, elementVarName);
        } else {
          ClassName mapItemClassName = getClassName(((AvroMapSchema) field.getSchemaOrRef().getDecl()).getValueSchema(), mapItemAvroType);
          customEncodeBuilder.addStatement("$T v2 = $L.getValue()", mapItemClassName, elementVarName);
        }
        populateCustomEncode(customEncodeBuilder, config, field, mapItemAvroType, valueVarName);
        customEncodeBuilder.endControlFlow();
        customEncodeBuilder.addStatement("out.writeMapEnd()");
        customEncodeBuilder.beginControlFlow("if ($L != $L)", actualSizeVarName, lengthVarName);
        customEncodeBuilder.addStatement("throw new $T(\"Map-size written was \" + $L + \", but element count was \" + $L + \".\")",
            ConcurrentModificationException.class, lengthVarName, actualSizeVarName);
        customEncodeBuilder.endControlFlow();
        sizeValCounter++;
        break;
      case UNION:
        int nullIndex = -1;
        int numberOfUnionMembers = ((AvroUnionSchema) field.getSchemaOrRef().getDecl()).getTypes().size();
        for (int i = 0; i < numberOfUnionMembers; i++) {
          SchemaOrRef unionMember  = ((AvroUnionSchema) field.getSchemaOrRef().getDecl()).getTypes().get(i);
          if(unionMember.getDecl().type().equals(AvroType.NULL)) {
            nullIndex = i;
          }
        }
        if(nullIndex != -1) {
          customEncodeBuilder
              .beginControlFlow("if (this.$L == null) ", field.getName())
              .addStatement("out.writeIndex($L)", nullIndex)
              .addStatement("out.writeNull()")
              .endControlFlow();
          if(numberOfUnionMembers > 1 ){
            customEncodeBuilder
                .beginControlFlow("else")
                .addStatement("out.writeIndex($L)", (nullIndex == 0) ? 1: 0);
            populateCustomEncodePrimitive(customEncodeBuilder,
                ((AvroUnionSchema) field.getSchemaOrRef().getDecl()).getTypes().get((nullIndex == 0) ? 1: 0).getDecl().type(), field.getName());
          }
          customEncodeBuilder.endControlFlow();
        } else {

          for(int i = 0; i < numberOfUnionMembers; i++) {
            SchemaOrRef unionMember  = ((AvroUnionSchema) field.getSchemaOrRef().getDecl()).getTypes().get(i);
            if (i == 0) {
              customEncodeBuilder.beginControlFlow("if (this.$L instanceof $T) ", fieldName,
                  avroTypeToJavaClass(unionMember.getSchema().type()));
            } else {
              customEncodeBuilder.endControlFlow();
              customEncodeBuilder.beginControlFlow(" else if (this.$L instanceof $T) ", fieldName,
                  avroTypeToJavaClass(unionMember.getSchema().type()));
            }
            populateCustomEncode(customEncodeBuilder, config, field, unionMember.getSchema().type(), fieldName);
          }
          customEncodeBuilder.endControlFlow();
        }
        break;
      case RECORD:
        customEncodeBuilder.addStatement("this.$L.customEncode(out)", field.getName());
        break;
      default:
        System.out.println("Unknown");
    }
  }

  private String getValueVarName() {
    return "v" + sizeValCounter;
  }

  private String getElementVarName() {
    return "e" + sizeValCounter;
  }

  private String getLengthVarName() {
    return "size" + sizeValCounter;
  }

  private String getActualSizeVarName() {
    return "actualSize" + sizeValCounter;
  }

  private void populateCustomEncodePrimitive(MethodSpec.Builder customEncodeBuilder, AvroType type, String fieldName) {
    switch (type) {
      case NULL:
        customEncodeBuilder.addStatement("out.writeNull()");
        break;
      case BOOLEAN:
        customEncodeBuilder.addStatement("out.writeBoolean($L)", fieldName);
        break;
      case INT:
        customEncodeBuilder.addStatement("out.writeInt($L)", fieldName);
        break;
      case LONG:
        customEncodeBuilder.addStatement("out.writeLong($L)", fieldName);
        break;
      case FLOAT:
        customEncodeBuilder.addStatement("out.writeFloat($L)", fieldName);
        break;
      case STRING:
        customEncodeBuilder.addStatement("out.writeString($L)", fieldName);
        break;
      case DOUBLE:
        customEncodeBuilder.addStatement("out.writeDouble($L)", fieldName);
        break;
      case BYTES:
        customEncodeBuilder.addStatement("out.writeBytes($L)", fieldName);
        break;

    }
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
        cls = java.lang.String.class;
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
        .returns(Object.class)
        .addParameter(Integer.class, "field")
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
            getClassName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type()));
      }
    }
    switchBuilder.addStatement("default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\");")
        .endControlFlow();

    classBuilder.addMethod(methodSpecBuilder.addCode(switchBuilder.build()).build());
  }

  private void addGetByIndexMethod(TypeSpec.Builder classBuilder, AvroRecordSchema recordSchema) {
    int fieldIndex = 0;
    MethodSpec.Builder methodSpecBuilder = MethodSpec.methodBuilder("get")
        .returns(Object.class)
        .addParameter(Integer.class, "field")
        .addModifiers(Modifier.PUBLIC);
    CodeBlock.Builder switchBuilder = CodeBlock.builder();
    switchBuilder.beginControlFlow("switch (field)");
    for (AvroSchemaField field : recordSchema.getFields()) {
      switchBuilder.addStatement("case $L: return $L", fieldIndex++, field.getName());
    }
    switchBuilder.addStatement("default: throw new org.apache.avro.AvroRuntimeException(\"Bad index\");")
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

    for(ClassName classNameToQualify: fullyQualifiedClassNamesInRecord) {

      if(!fieldNamesInRecord.contains(classNameToQualify.packageName().split("\\.")[0])){
        classBuilder.alwaysQualify(classNameToQualify.simpleName());
      }
    }
  }

  private void addFullyQualified(AvroSchemaField field, SpecificRecordGenerationConfig config) {
    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        fullyQualifiedClassesInRecord.add(fieldClass);
      } else {
        fullyQualifiedClassNamesInRecord.add(getClassName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type()));
      }
    }
  }

  private MethodSpec getSetterMethodSpec(AvroSchemaField field, SpecificRecordGenerationConfig config) {

    MethodSpec.Builder methodSpecBuilder = MethodSpec
        .methodBuilder("set"+field.getName())
        .addStatement("this.$L = $L", field.getName(), field.getName())
        .addModifiers(Modifier.PUBLIC);

    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultFieldStringRepresentation());
      if (fieldClass != null) {
        methodSpecBuilder.addParameter(fieldClass, field.getName())
            .addStatement("this.$L = $L", field.getName(), field.getName())
            .addModifiers(Modifier.PUBLIC);
      } else {
        ClassName className = getClassName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
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
        .methodBuilder("get"+field.getName())
        .addStatement("return this.$L()", field.getName()).addModifiers(Modifier.PUBLIC);

    if(field.getSchemaOrRef().getDecl() != null) {
      Class<?> fieldClass = getFieldClass(field.getSchemaOrRef().getDecl().type(), config.getDefaultMethodStringRepresentation());
      if (fieldClass != null) {
        methodSpecBuilder.returns(fieldClass);
      } else {
        ClassName className = getClassName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
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
        ClassName className = getClassName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
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
        ClassName className = getClassName(field.getSchemaOrRef().getDecl(), field.getSchemaOrRef().getDecl().type());
        parameterSpecBuilder = ParameterSpec.builder(className, field.getName());
      }
    } else {
      ClassName className =  ClassName.get(field.getSchemaOrRef().getParentNamespace(), field.getSchemaOrRef().getRef());
      parameterSpecBuilder = ParameterSpec.builder(className, field.getName());
    }
    return parameterSpecBuilder.build();
  }

  private ClassName getClassName(AvroSchema fieldSchema, AvroType avroType) {
    ClassName className = ClassName.OBJECT;
    switch (avroType) {
      case ENUM:
        AvroName enumName = ((AvroEnumSchema) fieldSchema).getName();
        className = ClassName.get(enumName.getNamespace(), enumName.getSimpleName());
        break;
      case FIXED:
        AvroName fixedName = ((AvroFixedSchema) fieldSchema).getName();
        className = ClassName.get(fixedName.getNamespace(), fixedName.getSimpleName());
    }
    return className;
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
        switch (defaultFieldStringRepresentation) {
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
