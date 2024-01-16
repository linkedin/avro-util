/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.codegen;

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
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.util.Utf8;


public class SpecificRecordGeneratorUtil {

  /***
   * Pattern to match single instance of $ sign
   */
  public static final Pattern SINGLE_DOLLAR_SIGN_REGEX = Pattern.compile("(?<![\\$])[\\$](?![\\$])");

  public static final String AVRO_GEN_COMMENT = "GENERATED CODE by avro-util";

  public static final ClassName CLASSNAME_SCHEMA = ClassName.get("org.apache.avro", "Schema");
  public static final ClassName CLASSNAME_SCHEMA_FIELD = ClassName.get(Schema.Field.class);
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

  public static final String ARRAY_GET_ELEMENT_TYPE = ".getElementType()";
  public static final String MAP_GET_VALUE_TYPE = ".getValueType()";

  private SpecificRecordGeneratorUtil(){}

  /***
   * Returns true if schema is union with size = 2 and a null member
   * @param schema
   * @return
   */
  public static boolean isSingleTypeNullableUnionSchema(AvroSchema schema) {
    if(!(schema instanceof AvroUnionSchema)) return false;
    AvroUnionSchema unionSchema = (AvroUnionSchema) schema;
    if(unionSchema.getTypes().size() == 2) {
      for(SchemaOrRef unionMember : unionSchema.getTypes()) {
        if(AvroType.NULL.equals(unionMember.getSchema().type())) {
          return true;
        }
      }
    }
    return false;
  }

  @Nullable
  public static Class<?> getJavaClassForAvroTypeIfApplicable(AvroType fieldType,
      AvroJavaStringRepresentation defaultStringRepresentation, boolean forComplexType) {
    Class<?> fieldClass = null;
    switch (fieldType) {
      case NULL:
        fieldClass = Void.class;
        break;
      case BOOLEAN:
        fieldClass = forComplexType ? Boolean.class : boolean.class;
        break;
      case INT:
        fieldClass =  forComplexType ? Integer.class : int.class;
        break;
      case LONG:
        fieldClass =  forComplexType ? Long.class : long.class;
        break;
      case FLOAT:
        fieldClass =  forComplexType ? Float.class : float.class;
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
        fieldClass = forComplexType ? Double.class : double.class;
        break;
      case BYTES:
        fieldClass = ByteBuffer.class;
        break;

    }
    return fieldClass;
  }

  public static TypeName getTypeName(AvroSchema fieldSchema, AvroType avroType, boolean getParameterizedTypeNames,
      AvroJavaStringRepresentation defaultStringRepresentation) {
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
        // if union is [type, null] or [null, type], className can be type. Else Object.class
        AvroUnionSchema unionSchema = (AvroUnionSchema) fieldSchema;

        if(isSingleTypeNullableUnionSchema(unionSchema)) {
          List<SchemaOrRef> branches = unionSchema.getTypes();
          SchemaOrRef unionMemberSchemaOrRef =
              (branches.get(0).getSchema().type().equals(AvroType.NULL) ? branches.get(1) : branches.get(0));

          AvroSchema branchSchema = unionMemberSchemaOrRef.getSchema();
          AvroType branchSchemaType = branchSchema.type();
          Class<?> simpleClass = getJavaClassForAvroTypeIfApplicable(branchSchemaType, defaultStringRepresentation, true);
          if (simpleClass != null) {
            className = ClassName.get(simpleClass);
          } else {
            className = getTypeName(branchSchema, branchSchemaType, getParameterizedTypeNames, defaultStringRepresentation);
          }
        } //otherwise Object
        break;
      case ARRAY:
        if(!getParameterizedTypeNames) return TypeName.get(List.class);
        AvroArraySchema arraySchema = ((AvroArraySchema) fieldSchema);
        Class<?> valueClass = getJavaClassForAvroTypeIfApplicable(arraySchema.getValueSchema().type(), defaultStringRepresentation, true);
        if (valueClass == null) {
          TypeName parameterTypeName = getTypeName(arraySchema.getValueSchema(), arraySchema.getValueSchema().type(),
              true, defaultStringRepresentation);
          className = ParameterizedTypeName.get(ClassName.get(List.class), parameterTypeName);
        } else {
          className = ParameterizedTypeName.get(List.class, valueClass);
        }
        break;
      case MAP:
        if(!getParameterizedTypeNames) return TypeName.get(Map.class);
        AvroMapSchema mapSchema = ((AvroMapSchema) fieldSchema);
        Class<?> mapValueClass = getJavaClassForAvroTypeIfApplicable(mapSchema.getValueSchema().type(), defaultStringRepresentation, true);
        Class<?> mapKeyClass = getJavaClassForAvroTypeIfApplicable(AvroType.STRING, defaultStringRepresentation, true);
        //complex map is allowed
        if (mapValueClass == null) {
          TypeName mapValueTypeName = getTypeName(mapSchema.getValueSchema(), mapSchema.getValueSchema().type(), true, defaultStringRepresentation);
          className =
              ParameterizedTypeName.get(ClassName.get(Map.class), TypeName.get(mapKeyClass), mapValueTypeName);
        } else {
          className = ParameterizedTypeName.get(Map.class, mapKeyClass, mapValueClass);
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
   */
  public static List<AvroNamedSchema> getNestedInternalSchemaList(AvroNamedSchema topLevelSchema) {

    AvroType type = topLevelSchema.type();
    switch (type) {
      case ENUM:
      case FIXED:
        return Collections.emptyList();
      case RECORD:
        return getNestedInternalSchemaListForRecord((AvroRecordSchema) topLevelSchema);
      default:
        throw new IllegalArgumentException("cant generate java class for " + type);
    }
  }

  /***
   * Checks if field type can be treated as null union of the given type
   *
   * @return True if type [null, type] or [type, null]
   */
  public static boolean isNullUnionOf(AvroType type, AvroSchema schema) {
    if(schema == null) return false;

    if(schema instanceof AvroUnionSchema) {
      List<SchemaOrRef> unionMembers = ((AvroUnionSchema) schema).getTypes();
      return
          (unionMembers.size() == 2 &&
          (
              (unionMembers.get(0).getSchema().type().equals(AvroType.NULL) && unionMembers.get(1).getSchema().type().equals(type))
                  ||
              (unionMembers.get(1).getSchema().type().equals(AvroType.NULL) && unionMembers.get(0).getSchema().type().equals(type))
          ));
    }
    return schema.type().equals(type);
  }

  /***
   * Handles list , union of list
   * @return true for List of String and List of Union of String
   */
  public static boolean isListTransformerApplicableForSchema(AvroSchema schema) {
    if(schema == null) return false;
    return isNullUnionOf(AvroType.ARRAY, schema) && schemaContainsString(schema);
  }

  public static boolean isMapTransformerApplicable(AvroSchema schema) {
    return isNullUnionOf(AvroType.MAP, schema);
  }


  public static boolean schemaContainsString(AvroSchema schema) {
    if (schema == null) {
      return false;
    }
    boolean hasString = false;
    switch (schema.type()) {
      case STRING:
      case MAP:
        return true;
      case UNION:
        AvroUnionSchema unionSchema = (AvroUnionSchema) schema;
        // Any member can have string?
        for(SchemaOrRef schemaOrRef : unionSchema.getTypes()) {
          hasString |= schemaContainsString(schemaOrRef.getSchema());
        }
        return hasString;
      case ARRAY:
        AvroArraySchema arraySchema = (AvroArraySchema) schema;
        return schemaContainsString(arraySchema.getValueSchema());
    }

    return false;
  }

  private static List<AvroNamedSchema> getNestedInternalSchemaListForRecord(AvroRecordSchema topLevelSchema) {
    List<AvroNamedSchema> schemaList = new ArrayList<>();
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

  public static String removePrefixFromFieldName(String fieldNameWithPrefix) {
    String[] fieldNameParts = fieldNameWithPrefix.split("\\.");
    return fieldNameParts.length > 1 ? fieldNameParts[1] : fieldNameWithPrefix;
  }

  public static boolean recordHasSimpleStringField(AvroRecordSchema schema) {
    for(AvroSchemaField field : schema.getFields()) {
      if(isNullUnionOf(AvroType.STRING, field.getSchema())) {
        return true;
      }
    }
    return false;
  }

}
