/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;


/**
 * utility class for generating random (valid) records given a schema.
 * useful for testing
 */
public class RandomRecordGenerator {

  /**
   * creates a random (generic) instance of a schema
   * @param of schema to generate a random specimen of
   * @return a random specimen
   */
  public Object randomGeneric(Schema of) {
    return randomGeneric(of, RecordGenerationConfig.newConfig());
  }

  public Object randomGeneric(Schema of, RecordGenerationConfig config) {
    RecordGenerationContext context = new RecordGenerationContext(config);
    return newRandomGeneric(of, context);
  }

  public <T> T randomSpecific(Class<T> specificClass) {
    return randomSpecific(specificClass, RecordGenerationConfig.newConfig());
  }

  @SuppressWarnings("unchecked")
  public <T> T randomSpecific(Class<T> specificClass, RecordGenerationConfig config) {
    RecordGenerationContext context = new RecordGenerationContext(config);
    Schema declaredSchema = AvroSchemaUtil.getDeclaredSchema(specificClass);
    if (declaredSchema == null) {
      throw new IllegalStateException("unable to determine declared schema for class " + specificClass.getName());
    }
    return (T) newRandomSpecific(specificClass, declaredSchema, context);
  }

  private Object newRandomGeneric(Schema of, RecordGenerationContext context) {
    RecordGenerationConfig config = context.getConfig();
    Random random = config.random();
    int size;
    byte[] randomBytes;
    int index;
    StringRepresentation stringRep = config.preferredStringRepresentation();
    Schema popped;

    //TODO - logical types?
    switch (of.getType()) {
      case NULL:
        return null;
      case BOOLEAN:
        return random.nextBoolean();
      case INT:
        return random.nextInt();
      case LONG:
        return random.nextLong();
      case FLOAT:
        return random.nextFloat();
      case DOUBLE:
        return random.nextDouble();
      case BYTES:
        size = random.nextInt(11); //[0, 10]
        randomBytes = new byte[size];
        random.nextBytes(randomBytes);
        return ByteBuffer.wrap(randomBytes);
      case STRING:
        size = random.nextInt(11); //[0, 10]
        StringBuilder sb = new StringBuilder(size);
        //return alphanumeric string of size
        random.ints(size, '0', 'z' + 1).forEachOrdered(sb::appendCodePoint);
        return convertStringToType(sb.toString(), stringRep);
      case FIXED:
        size = of.getFixedSize();
        randomBytes = new byte[size];
        random.nextBytes(randomBytes);
        return AvroCompatibilityHelper.newFixed(of, randomBytes);
      case ENUM:
        List<String> symbols = of.getEnumSymbols();
        index = random.nextInt(symbols.size());
        return AvroCompatibilityHelper.newEnumSymbol(of, symbols.get(index));
      case RECORD:
        context.pushPath(of);
        GenericData.Record record = new GenericData.Record(of);
        for (Schema.Field field : of.getFields()) {
          //TODO - extend to allow (multiple-hop-long) self-references to complete the experience :-)
          Schema fieldSchema = field.schema();
          Object randomValue = newRandomGeneric(fieldSchema, context);
          record.put(field.pos(), randomValue);
        }
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return record;
      case ARRAY:
        if (context.hasHitMaxRecursiveDepth()) {
          return new GenericData.Array<>(0, of);
        }
        context.pushPath(of);
        size = random.nextInt(11); //[0, 10]
        GenericData.Array<Object> array = new GenericData.Array<>(size, of);
        Schema elementType = of.getElementType();
        for (int i = 0; i < size; i++) {
          array.add(newRandomGeneric(elementType, context));
        }
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return array;
      case MAP:
        if (context.hasHitMaxRecursiveDepth()) {
          return new HashMap<>(0);
        }
        context.pushPath(of);
        size = random.nextInt(11); //[0, 10]
        HashMap<CharSequence, Object> map = new HashMap<>(size);
        Schema valueType = of.getValueType();
        for (int i = 0; i < size; i++) {
          String key = "key-" + i; //TODO - better randomness (yet results should be unique)
          map.put(convertStringToType(key, stringRep), newRandomGeneric(valueType, context));
        }
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return map;
      case UNION:
        if (context.hasHitMaxRecursiveDepth() && AvroSchemaUtil.isNullAValidDefaultForSchema(of)) {
          return null;
        }
        context.pushPath(of);
        List<Schema> acceptableBranches = narrowDownUnionBranches(of, of.getTypes(), context);
        index = random.nextInt(acceptableBranches.size());
        Schema branch = acceptableBranches.get(index);
        Object value = newRandomGeneric(branch, context);
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return value;
      default:
        throw new UnsupportedOperationException("unhandled: " + of.getType());
    }
  }

  /**
   *
   * @param clazz (optional) the return value should be an instanceof this class. null means the method can return
   *              an instance of any type it sees fit (according to the avro schema)
   * @param of avro schema of desired return value
   * @param context context for this operation (which includes config)
   * @return a returned value that conforms to both the desired avro schema and java class
   */
  private Object newRandomSpecific(Class<?> clazz, Schema of, RecordGenerationContext context) {
    RecordGenerationConfig config = context.getConfig();
    Random random = config.random();
    int size;
    byte[] randomBytes;
    int index;
    StringRepresentation stringRep = config.preferredStringRepresentation();
    Schema popped;

    //TODO - logical types !?
    switch (of.getType()) {
      case NULL:
        return null;
      case BOOLEAN:
        return random.nextBoolean();
      case INT:
        return random.nextInt();
      case LONG:
        return random.nextLong();
      case FLOAT:
        return random.nextFloat();
      case DOUBLE:
        return random.nextDouble();
      case BYTES:
        size = random.nextInt(11); //[0, 10]
        randomBytes = new byte[size];
        random.nextBytes(randomBytes);
        return ByteBuffer.wrap(randomBytes);
      case STRING:

        if (clazz != null) {
          if (clazz.equals(Utf8.class)) {
            stringRep = StringRepresentation.Utf8;
          } else if (clazz.equals(String.class)) {
            stringRep = StringRepresentation.String;
          } else if (!clazz.equals(CharSequence.class)) { //CharSequence means we use preferred rep
            throw new IllegalStateException("dont know how to generate a string of type " + clazz.getName());
          }
        }

        size = random.nextInt(11); //[0, 10]
        StringBuilder sb = new StringBuilder(size);
        //return alphanumeric string of size
        random.ints(size, '0', 'z' + 1).forEachOrdered(sb::appendCodePoint);

        return convertStringToType(sb.toString(), stringRep);
      case FIXED:
        size = of.getFixedSize();
        randomBytes = new byte[size];
        random.nextBytes(randomBytes);
        Class<?> fixedClass = clazz;
        if (fixedClass == null) {
          fixedClass = findClassForSchema(of);
        }
        try {
          Constructor<?> ctr = fixedClass.getConstructor(BYTE_ARRAY_ARG);
          //noinspection PrimitiveArrayArgumentToVarargsMethod
          return ctr.newInstance(randomBytes);
        } catch (Exception e) {
          throw new IllegalStateException("failed to find constructor or construct fixed class " + fixedClass.getName(), e);
        }
      case ENUM:
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<? extends Enum> enumClass = (Class<? extends Enum>) clazz;
        if (enumClass == null) {
          //noinspection unchecked,rawtypes
          enumClass = (Class<? extends Enum>) findClassForSchema(of);
        }
        @SuppressWarnings("unchecked")
        Object[] symbols = getEnumValues(enumClass);
        index = random.nextInt(symbols.length);
        return symbols[index];
      case RECORD:
        context.pushPath(of);
        Class<?> recordClass = clazz;
        if (recordClass == null) {
          recordClass = findClassForSchema(of);
        }
        List<Schema.Field> fields = of.getFields();
        IndexedRecord record = (IndexedRecord) instantiate(recordClass, of);
        for (Schema.Field field : fields) {
          //TODO - extend to allow (multiple-hop-long) self-references to complete the experience :-)
          Schema fieldSchema = field.schema();
          Class<?> expectedClass = expectedFieldType(recordClass, field);
          Object randomValue = newRandomSpecific(expectedClass, fieldSchema, context);
          record.put(field.pos(), randomValue);
        }
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return record;
      case ARRAY:
        context.pushPath(of);
        size = random.nextInt(11); //[0, 10]
        ArrayList<Object> array = new ArrayList<>(size);
        //we cant get the generic info from the class argument (that i know of?)
        //we we take the element type from the schema and hope for the best
        Schema elementType = of.getElementType();
        for (int i = 0; i < size; i++) {
          array.add(newRandomSpecific(null, elementType, context));
        }
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return array;
      case MAP:
        context.pushPath(of);
        size = random.nextInt(11); //[0, 10]
        HashMap<CharSequence, Object> map = new HashMap<>(size);
        Schema valueType = of.getValueType();
        //we cant get the generic info from the class argument (that i know of?)
        //we we take the element type from the schema and hope for the best
        for (int i = 0; i < size; i++) {
          String key = "key-" + i; //TODO - better randomness (yet results should be unique)
          map.put(convertStringToType(key, stringRep), newRandomSpecific(null, valueType, context));
        }
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return map;
      case UNION:
        context.pushPath(of);
        List<Schema> acceptableBranches = narrowDownUnionBranches(of, of.getTypes(), context);
        index = random.nextInt(acceptableBranches.size());
        Schema branch = acceptableBranches.get(index);
        Object value = newRandomSpecific(null, branch, context); //TODO - find class?
        popped = context.popPath();
        if (popped != of) {
          throw new IllegalStateException("bug in path handling, pushed " + of + " but popped " + popped);
        }
        return value;
      default:
        throw new UnsupportedOperationException("unhandled: " + of.getType());
    }
  }

  /**
   * given a union schema, narrow down "acceptable" union branches that match the given generation config
   * @param unionSchema union schema
   * @param branches branches of the union schema
   * @param context operation context (which will include config)
   * @return acceptable branches. throws an exception if no branches meet criteria
   */
  private List<Schema> narrowDownUnionBranches(Schema unionSchema, List<Schema> branches, RecordGenerationContext context) {
    RecordGenerationConfig config = context.getConfig();
    List<Schema> results = new ArrayList<>(branches.size());

    //first of all go over all branches and pick those that are not loops, and at some (decaying) probability also those that are loops
    Schema unionNullBranch = null;
    for (Schema branch : branches) {
      if (branch.getType() == Schema.Type.NULL) {
        //handle nulls later
        unionNullBranch = branch;
        continue;
      }
      int loopDepth = context.seen(branch);
      if (loopDepth == 0) {
        results.add(branch); //never seen this path
        continue;
      }
      if (config.random().nextDouble() > 1.0 - config.selfReferenceProbability()) {
        results.add(branch);
      }
    }
    //null branch is a candidate if it exists and config allows it or no other choice (due to loop elimination)
    if (unionNullBranch != null && (!config.avoidNulls() || results.isEmpty())) {
      results.add(unionNullBranch);
    }

    if (results.isEmpty()) {
      throw new IllegalStateException("no acceptable union branches out of original " + unionSchema);
    }
    return results;
  }

  private static final Class<?>[] NO_ARGS = new Class[0];
  private static final Class<?>[] BYTE_ARRAY_ARG = new Class[] { byte[].class };

  private <T> T instantiate(Class<T> clazz, Schema of) {
    if (clazz == null) {
      throw new IllegalArgumentException("clazz argument required");
    }
    //TODO - look for both old and new SchemaConstructable ctrs 1st
    try {
      Constructor<T> noArgCtr = clazz.getDeclaredConstructor(NO_ARGS);
      return noArgCtr.newInstance(NO_ARGS);
    } catch (Exception e) {
      throw new IllegalStateException("while trying to instantiate a(n) " + clazz.getName(), e);
    }
  }

  private Class<?> expectedFieldType(Class<?> recordClass, Schema.Field field) {
    String fieldName = field.name();
    Exception issue = null;
    //look for a public field by name directly
    Field pojoField = null;
    try {
      pojoField = recordClass.getField(fieldName);
    } catch (Exception e) {
      issue = e;
    }
    if (pojoField != null) {
      return pojoField.getType();
    }
    //look for "mangled" field if field name is possibly reserved
    if (AvroRecordUtil.AVRO_RESERVED_FIELD_NAMES.contains(fieldName.toLowerCase(Locale.ROOT))) {
      String mangled = fieldName + "$";
      try {
        pojoField = recordClass.getField(mangled);
      } catch (Exception e) {
        issue.addSuppressed(e); //!= null
      }
      if (pojoField != null) {
        return pojoField.getType();
      }
    }
    //TODO - look for setter
    throw new IllegalStateException("unable to find public field " + fieldName + " on class " + recordClass.getName(), issue);
  }

  private <T extends Enum<T>> T[] getEnumValues(Class<T> enumClass) {
    try {
      Method valuesMethod = enumClass.getMethod("values", NO_ARGS);
      @SuppressWarnings("unchecked")
      T[] values = (T[]) valuesMethod.invoke(null);
      return values;
    } catch (Exception e) {
      throw new IllegalStateException("unable to get values of enum(?) class " + enumClass.getName(), e);
    }
  }

  private Class<?> findClassForSchema(Schema schema) {
    switch (schema.getType()) {
      case ENUM:
      case FIXED:
      case RECORD:
        String fqcn = schema.getFullName();
        try {
          return ClassLoaderUtil.forName(fqcn);
        } catch (Exception e) {
          throw new IllegalStateException("error loading class " + fqcn, e);
        }
      default:
        throw new IllegalStateException("dont know how to find specific class for schema of type " + schema.getType());
    }
  }

  private CharSequence convertStringToType(String str, StringRepresentation type) {
    switch (type) {
      case String:
        return str;
      case Utf8:
        return new Utf8(str);
      default:
        throw new IllegalStateException("unexpected value " + type);
    }
  }
}
