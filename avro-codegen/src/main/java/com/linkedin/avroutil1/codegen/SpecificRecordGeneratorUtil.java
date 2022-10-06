/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

import com.linkedin.avroutil1.compatibility.CompatibleSpecificRecordBuilderBase;
import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroName;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.reflect.Nullable;


public class SpecificRecordGeneratorUtil {

  /***
   * Pattern to match single instance of $ sign
   */
  public static final Pattern SINGLE_DOLLAR_SIGN_REGEX = Pattern.compile("(?<![\\$])[\\$](?![\\$])");

  public static final String AVRO_GEN_COMMENT = "GENERATED CODE by avro-util";

  public static final ClassName CLASSNAME_SCHEMA = ClassName.get("org.apache.avro", "Schema");
  public static final ClassName CLASSNAME_SPECIFIC_DATA = ClassName.get("org.apache.avro.specific", "SpecificData");
  public static final ClassName CLASSNAME_SPECIFIC_RECORD = ClassName.get("org.apache.avro.specific", "SpecificRecord");
  public static final ClassName CLASSNAME_SPECIFIC_RECORD_BASE = ClassName.get("org.apache.avro.specific", "SpecificRecordBase");
  public static final ClassName CLASSNAME_SPECIFIC_DATUM_READER = ClassName.get("org.apache.avro.specific", "SpecificDatumReader");
  public static final ClassName CLASSNAME_SPECIFIC_DATUM_WRITER = ClassName.get("org.apache.avro.specific", "SpecificDatumWriter");
  public static final ClassName CLASSNAME_ENCODER = ClassName.get("org.apache.avro.io", "Encoder");
  public static final ClassName CLASSNAME_RESOLVING_DECODER = ClassName.get("org.apache.avro.io", "ResolvingDecoder");
  public static final ClassName CLASSNAME_DATUM_READER = ClassName.get("org.apache.avro.io", "DatumReader");
  public static final ClassName CLASSNAME_DATUM_WRITER = ClassName.get("org.apache.avro.io", "DatumWriter");
  public static final ClassName CLASSNAME_FIXED_SIZE = ClassName.get("org.apache.avro.specific", "FixedSize");
  public static final ClassName CLASSNAME_SPECIFIC_FIXED = ClassName.get("org.apache.avro.specific", "SpecificFixed");


  public static HashSet<TypeName> fullyQualifiedClassNamesInRecord = new HashSet<>();

  public static HashSet<String> fullyQualifiedClassesInRecord = new HashSet<>(Arrays.asList(
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

  private SpecificRecordGeneratorUtil(){}


  private static boolean isSingleTypeNullableUnionSchema(AvroUnionSchema unionSchema) {
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

  /***
   *
   * @param avroType
   * @return
   *      Returns Java class for matching types
   *      Returns null if TypeName should be used instead
   */
  @Nullable
  public static Class<?> getJavaClassForAvroTypeIfApplicable(AvroType avroType) {
    Class<?> cls;
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
      default:
        cls = null;
    }
    return cls;
  }
  public static TypeName getTypeName(AvroSchema fieldSchema, AvroType avroType, boolean getParameterizedTypeNames) {
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
            className = getTypeName(branchSchema, branchSchemaType, getParameterizedTypeNames);
          }
        } //otherwise Object
        break;
      case ARRAY:
        if(!getParameterizedTypeNames) return TypeName.get(List.class);
        AvroArraySchema arraySchema = ((AvroArraySchema) fieldSchema);
        Class<?> valueClass = getJavaClassForAvroTypeIfApplicable(arraySchema.getValueSchema().type());
        if (valueClass == null) {
          TypeName parameterTypeName = getTypeName(arraySchema.getValueSchema(), arraySchema.getValueSchema().type(),
              true);
          fullyQualifiedClassesInRecord.add(parameterTypeName.toString());
          className = ParameterizedTypeName.get(ClassName.get(List.class), parameterTypeName);
        } else {
          className = ParameterizedTypeName.get(List.class, valueClass);
        }
        break;
      case MAP:
        if(!getParameterizedTypeNames) return TypeName.get(Map.class);
        AvroMapSchema mapSchema = ((AvroMapSchema) fieldSchema);
        Class<?> mapValueClass = getJavaClassForAvroTypeIfApplicable(mapSchema.getValueSchema().type());
        //complex map is allowed
        if (mapValueClass == null) {
          TypeName mapValueTypeName = getTypeName(mapSchema.getValueSchema(), mapSchema.getValueSchema().type(), true);
          fullyQualifiedClassesInRecord.add(mapValueTypeName.toString());
          className =
              ParameterizedTypeName.get(ClassName.get(Map.class), TypeName.get(CharSequence.class), mapValueTypeName);
        } else {
          className = ParameterizedTypeName.get(Map.class, CharSequence.class, mapValueClass);
        }
        break;
      default:
        break;
    }
    return className;
  }

  /***
   * Get nested internal Named schemas for a given topLevel Record schema
   * Traverses through fields and catches their de-duped
   * @param topLevelSchema
   * @return
   */
  public static List<AvroNamedSchema> getNestedInternalSchemaList(AvroNamedSchema topLevelSchema) {

    AvroType type = topLevelSchema.type();
    switch (type) {
      case ENUM:
      case FIXED:
        return new ArrayList<>();
      case RECORD:
        return getNestedInternalSchemaListForRecord((AvroRecordSchema) topLevelSchema);
      default:
        throw new IllegalArgumentException("cant generate java class for " + type);
    }
  }

  private static List<AvroNamedSchema> getNestedInternalSchemaListForRecord(AvroRecordSchema topLevelSchema) {
    List<AvroNamedSchema> schemaList = new ArrayList<>();
    schemaList.add(topLevelSchema);
    HashSet<String> visitedSchemasFullNames = new HashSet<>();
    Queue<AvroSchema> schemaQueue = topLevelSchema.getFields()
        .stream()
        .filter(field -> field.getSchemaOrRef().getRef() == null)
        .map(AvroSchemaField::getSchema)
        .collect(Collectors.toCollection(LinkedList::new));

    while (!schemaQueue.isEmpty()) {
      AvroSchema fieldSchema = schemaQueue.poll();

      //De-dup using visited schema
      if(fieldSchema instanceof AvroNamedSchema) {
        String fieldFullName = ((AvroNamedSchema) fieldSchema).getFullName();
        if (visitedSchemasFullNames.contains(fieldFullName)) {
          continue;
        } else {
          schemaList.add((AvroNamedSchema) fieldSchema);
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
          break;
        case UNION:
          // add union members to fields queue
          ((AvroUnionSchema) fieldSchema).getTypes().forEach(unionMember -> {
            if (AvroType.NULL != unionMember.getSchema().type() && unionMember.getRef() == null) {
              schemaQueue.add(unionMember.getSchema());
            }
          });
          break;
        case MAP:
          schemaQueue.add(((AvroMapSchema) fieldSchema).getValueSchema());
          break;
        case ARRAY:
          schemaQueue.add(((AvroArraySchema) fieldSchema).getValueSchema());
          break;
      }
    }
    return schemaList;
  }

}
