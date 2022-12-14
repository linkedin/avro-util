/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.compatibility.collectiontransformer.ObjectTransformer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.StringJoiner;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


/**
 * utility class to allow access to a field
 * on some generated class without worrying
 * about "how" (public fields vs getters/setters,
 * string types, etc?)
 */
public class FieldAccessor {
  /**
   * (generated) record class
   */
  private final Class<? extends SpecificRecord> recordClass;

  private final Schema recordSchema;

  private final Schema.Field field;
  /**
   * public clas field (null if none)
   */
  private final Field publicField;
  /**
   * public setter method (null if none)
   */
  private final Method setter;

  public FieldAccessor(
      Class<? extends SpecificRecord> recordClass,
      Schema recordSchema,
      Schema.Field field,
      Field publicField,
      Method setter
  ) {
    if (recordClass == null) {
      throw new IllegalArgumentException("record argument required");
    }
    if (recordSchema == null) {
      throw new IllegalArgumentException("recordSchema argument required");
    }
    if (field == null) {
      throw new IllegalArgumentException("field argument required");
    }
    this.recordClass = recordClass;
    this.recordSchema = recordSchema;
    this.field = field;
    this.publicField = publicField;
    this.setter = setter;
  }

  public void setValue(SpecificRecord record, Object value) {
    Schema destinationSchema = field.schema(); //unless union (see below)
    if (destinationSchema.getType() == Schema.Type.UNION) {
      //will throw if no matching branch found
      //TODO - catch and provide details of record.field ?
      destinationSchema = AvroSchemaUtil.resolveUnionBranchOf(value, destinationSchema);
    }
    boolean hasStrings = AvroSchemaUtil.schemaContainsString(destinationSchema);
    if (hasStrings) {
      StringRepresentation fieldRep = null;
      if (publicField != null) {
        fieldRep = stringRepForField(publicField);
      }
      StringRepresentation setterRep = null;
      if (setter != null) {
        setterRep = stringRepForSetter(setter);
      }

      //try field 1st
      if (fieldRep != null) {
        Object transformed = ObjectTransformer.transform(value, fieldRep);
        try {
          publicField.set(record, transformed);
        } catch (Exception e) {
          throw new IllegalArgumentException("while setting field " + publicField + " with transformed payload " + transformed, e);
        }
        return;
      }
      //try setter 2nd
      if (setterRep != null) {
        Object transformed = ObjectTransformer.transform(value, setterRep);
        try {
          setter.invoke(record, transformed);
        } catch (Exception e) {
          throw new IllegalArgumentException("while calling setter " + setter + " with transformed payload " + transformed, e);
        }
      }
    } else {
      //try field 1st
      if (publicField != null) {
        try {
          publicField.set(record, value);
        } catch (Exception e) {
          throw new IllegalArgumentException("while setting field " + publicField + " with raw payload " + value, e);
        }
        return;
      }
      //try setter 2nd
      if (setter != null) {
        try {
          setter.invoke(record, value);
        } catch (Exception e) {
          throw new IllegalArgumentException("while calling setter " + setter + " with raw payload " + value, e);
        }
      }
    }
  }

  //package private ON PURPOSE
  static StringRepresentation stringRepForField(Field field) {
    Type fieldType = field.getGenericType();
    @SuppressWarnings("UnnecessaryLocalVariable")
    StringRepresentation rep = stringRepForType(fieldType);
    return rep;
  }

  //package private ON PURPOSE
  static StringRepresentation stringRepForSetter(Method setter) {
    Type[] genericParams = setter.getGenericParameterTypes();
    //noinspection ConstantConditions
    if (genericParams == null || genericParams.length != 1) {
      throw new IllegalStateException("generic args for setter " + setter + " expected to contain single type: "
          + toString(genericParams));
    }
    Type singleArg = genericParams[0];
    @SuppressWarnings("UnnecessaryLocalVariable")
    StringRepresentation rep = stringRepForType(singleArg);
    return rep;
  }

  //package private ON PURPOSE
  static StringRepresentation stringRepForType(Type type) {
    if (type == null) {
      throw new IllegalArgumentException("type required");
    }
    if (type instanceof Class) {
      @SuppressWarnings("UnnecessaryLocalVariable")
      StringRepresentation rep = StringRepresentation.forClass((Class<?>) type);
      return rep;
    }
    if (type instanceof ParameterizedType) {
      ParameterizedType complexType = (ParameterizedType) type;
      Type[] typeArgs = complexType.getActualTypeArguments();
      if (typeArgs == null || typeArgs.length == 0) {
        throw new IllegalStateException("type " + type + " has no type args?");
      }
      StringRepresentation rep = null;
      Type firstTypeWithRep = null;
      for (Type typeArg : typeArgs) {
        StringRepresentation fromArg = stringRepForType(typeArg);
        if (fromArg != null) {
          if (rep == null) {
            rep = fromArg;
            firstTypeWithRep = typeArg;
            continue;
          }
          if (!rep.equals(fromArg)) {
            throw new IllegalStateException("multiple string representations in " + type + ": "
                + firstTypeWithRep + " is " + rep + " but " + typeArg + " is " + fromArg);
          }
        }
      }
      return rep;
    }
    throw new IllegalStateException("dont know what to do with " + type.getClass().getName() + ": " + type);
  }

  private static String toString(Type[] arr) {
    if (arr == null) {
      return "null";
    }
    StringJoiner csv = new StringJoiner(", ");
    for (Type type : arr) {
      csv.add(String.valueOf(type));
    }
    return "[" + csv.toString() + "]";
  }
}
