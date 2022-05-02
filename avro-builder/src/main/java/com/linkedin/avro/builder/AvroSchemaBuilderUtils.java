/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.builder;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;

public class AvroSchemaBuilderUtils {

  private static final char[] DIGITS_LOWER =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  private static final Set<Schema.Type> AVRO_NAMED_TYPES = Collections.unmodifiableSet(new HashSet<>(
      Arrays.asList(Schema.Type.ENUM, Schema.Type.FIXED, Schema.Type.RECORD)));

  private AvroSchemaBuilderUtils() { }

  /**
   * returns an md5 binary hash of a schema by printing the schema
   * to json and hasing the resulting json
   * @param schema a schema object
   * @return md5 hash of normalized json form
   */
  public static byte[] schemaToMd5(Schema schema) {
    String asString = schemaToString(schema);
    byte[] utf8Bytes = AvroSchemaBuilderUtils.utf8(asString);
    return AvroSchemaBuilderUtils.md5(utf8Bytes);
  }

  /**
   * returns an md5 binary hash of a schema by parsing it, printing a normalized
   * json schema and hashing that schema.
   * <b>NOTE:</b> please dont use this if you have a Schema object already on hand
   * @param avroSchemaString a (json) schema string
   * @return md5 hash of normalized json form
   */
  public static byte[] schemaStringToMd5(String avroSchemaString) {
    // return the md5 from the canonized schema string
    return schemaToMd5(Schema.parse(avroSchemaString));
  }

  public static String schemaToString(Schema schema) {
    return schema.toString(false);
  }

  public static byte[] decode(String hex) {
    byte[] bytes = new byte[hex.length() / 2];

    for (int x = 0; x < bytes.length; x++) {
      bytes[x] = (byte) Integer.parseInt(hex.substring(2 * x, 2 * x + 2), 16);
    }

    return bytes;
  }

  public static String hex(byte[] bytes) {
    // Code copied from org.apache.commons.codec.binary.Hex#encodeHexString
    final int l = bytes.length;
    final char[] out = new char[l << 1];
    for (int i = 0, j = 0; i < l; i++) {
      out[j++] = DIGITS_LOWER[(0xF0 & bytes[i]) >>> 4];
      out[j++] = DIGITS_LOWER[0x0F & bytes[i]];
    }
    return new String(out);
  }

  public static byte[] md5(byte[] bytes) {
    try {
      MessageDigest digest = MessageDigest.getInstance("md5");
      return digest.digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("This can't happen.", e);
    }
  }

  public static byte[] utf8(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("This can't happen");
    }
  }

  public static <T> T notNull(T t) {
    if (t == null) {
      throw new IllegalArgumentException("Null value not allowed.");
    } else {
      return t;
    }
  }

  public static void croak(String message) {
    croak(message, 1);
  }

  public static void croak(String message, int exitCode) {
    System.err.println(message);
    System.exit(exitCode);
  }

  public static boolean equals(Object x, Object y) {
    if (x == y) {
      return true;
    } else if (x == null) {
      return false;
    } else if (y == null) {
      return false;
    } else {
      return x.equals(y);
    }
  }

  public static List<File> toFiles(List<String> strs) {
    List<File> files = new ArrayList<>();
    for (String s : strs) {
      files.add(new File(s));
    }
    return files;
  }

  public static boolean isParentOf(File maybeParent, File maybeChild) {
    return maybeChild.toPath().startsWith(maybeParent.toPath());
  }

  public static byte[] getBytes(String s, String e) {
    try {
      return s.getBytes(e);
    } catch (UnsupportedEncodingException exp) {
      throw new RuntimeException(exp);
    }
  }

  /**
   * This helper function copies the contents of a generic record into a specific record. This function is
   * useful in consuming records from a KafkaConsumer when you know the actual specific record type.  This function
   * assumes that the schema to java class mapping is already in the cache of SpecificData.
   *
   * @param specificRecord An empty specific record which data is copied into
   * @param genericRecord  The generic record that contains your data
   * @return the specific record
   * @throws AvroRuntimeException Throws an exception if there is a structural or type mismatch between the data and the specific record
   */
  public static <SR extends SpecificRecord> SR genericRecordToSpecificRecord(SR specificRecord,
      GenericData.Record genericRecord) throws AvroRuntimeException {
    return indexedRecordToSpecificRecord(specificRecord, genericRecord);
  }

  /**
   * This helper function copies the contents of an indexed record into a specific record. This function is
   * useful in consuming records from a KafkaConsumer when you know the actual specific record type.  This function
   * assumes that the schema to java class mapping is already in the cache of SpecificData.
   *
   * @param specificRecord An empty specific record which data is copied into
   * @param indexedRecord The indexed record that contains your data
   * @return the specific record
   * @throws AvroRuntimeException Throws an exception if there is a structural or type mismatch between the data and the specific record
   */
  public static <SR extends SpecificRecord> SR indexedRecordToSpecificRecord(SR specificRecord,
      IndexedRecord indexedRecord) throws AvroRuntimeException {
    return deepConvert(specificRecord, indexedRecord);
  }

  /**
   * This helper function copies the contents of an indexed record into another indexed record.  This function
   * assumes that the schema to java class mapping is already in the cache of SpecificData.
   *
   * @param destination An empty indexed record which data is copied into
   * @param source The indexed record that contains your data
   * @return destination, with the values of source copied
   * @throws AvroRuntimeException Throws an exception if there is a structural or type mismatch between the data and the specific record
   */
  public static <IR extends IndexedRecord> IR deepConvert(IR destination,
      IndexedRecord source) throws AvroRuntimeException {
    for (Schema.Field field : destination.getSchema().getFields()) {
      Object fieldData = getFieldValue(source, field);

      if (fieldData == null && AvroCompatibilityHelper.fieldHasDefault(field)) {
        fieldData = AvroCompatibilityHelper.getGenericDefaultValue(field);
      }

      final Object fieldValue;
      if (fieldData == null) {
        fieldValue = null;
      } else {
        final Schema fieldSchema = resolveSchema(fieldData, field.schema());
        fieldValue = interpretFieldData(fieldData, fieldSchema);
      }

      destination.put(field.pos(), fieldValue);
    }
    return destination;
  }

  /**
   * given a (parent) schema, and a field name, find the schema for that field.
   * if the field is a union, returns the (only) non-null branch of the union
   * @param parent schema
   * @param fieldName field in the given schema
   * @return schema type for the named field, or 1st non-null type if field is union
   */
  public static Schema findNonNullUnionBranch(Schema parent, String fieldName) {
    if (parent == null || fieldName == null || fieldName.isEmpty()) {
      throw new IllegalArgumentException("arguments must not be null/empty");
    }
    Schema.Field field = parent.getField(fieldName);
    if (field == null) {
      return null;
    }
    Schema fieldSchema = field.schema();
    Schema.Type fieldSchemaType = fieldSchema.getType();
    if (!Schema.Type.UNION.equals(fieldSchemaType)) {
      return fieldSchema; //field is not a union
    }
    List<Schema> branches = fieldSchema.getTypes();
    List<Schema> nonNullBranches = branches.stream().
        filter(schema -> !Schema.Type.NULL.equals(schema.getType())).collect(Collectors.toList());
    if (nonNullBranches.size() != 1) {
      throw new IllegalArgumentException(String.format("field %s has %d non-null union branches, where exactly 1 is expected in %s",
          fieldName, nonNullBranches.size(), parent));
    }
    return nonNullBranches.get(0);
  }

  /**
   * DFSs a schema, allowing a user-provided visitor to visit all fields
   * and nested schemas within
   * @param schema a schema
   * @param visitor a visitor to be called when visiting fields and nested schemas
   */
  public static void traverseSchema(Schema schema, SchemaVisitor visitor) {
    IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
    traverseSchema(schema, visitor, visited);
  }

  /**
   * given a set of root names schemas (which may contain more named schemas defined inline)
   * returns the set of all named schemas defined by the root schemas (including the
   * root schemas themselves) keyed by their full name
   * @param roots root named schemas
   * @return map of all schemas defined inside root, including root
   */
  public static Map<String, Schema> getAllDefinedSchemas(Collection<Schema> roots) {
    if (roots == null) {
      throw new IllegalArgumentException("argument must not be null");
    }
    Map<String, Schema> results = new HashMap<>(roots.size());
    for (Schema root : roots) {
      Map<String, Schema> definedByRoot = getAllDefinedSchemas(root);
      results.putAll(definedByRoot);
    }
    return results;
  }

  /**
   * given a root named schema (which may contain more named schemas defined inline)
   * returns the set of all named schemas defined by the root schema (including the
   * root schema itself) keyed by their full name
   * @param root root named schema (a record, enum, or fixed type)
   * @return map of all schemas defined inside root, including root
   */
  public static Map<String, Schema> getAllDefinedSchemas(Schema root) {
    if (root == null) {
      throw new IllegalArgumentException("argument must not be null");
    }
    if (!AVRO_NAMED_TYPES.contains(root.getType())) {
      throw new IllegalArgumentException("input schema is a " + root.getType() + " and not a named type - " + root);
    }
    IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
    final Map<String, Schema> results = new HashMap<>();
    SchemaVisitor visitor = new SchemaVisitor() {
      @Override
      public void visitSchema(Schema schema) {
        if (AVRO_NAMED_TYPES.contains(schema.getType())) {
          results.put(schema.getFullName(), schema);
        }
      }
    };
    traverseSchema(root, visitor, visited);
    return results;
  }

  /**
   * a more useful version of {@link GenericData#toString(Object)} for providing
   * the level of detail needed to root cause a few common issues:
   * <ul>
   *   <li>use of various types of strings</li>
   *   <li>use of string values vs enum symbols</li>
   *   <li>use of generic vs specific records</li>
   * </ul>
   *
   * example output:
   * {
   *  "modelVersion": "1.2.3" (a string of type org.apache.avro.util.Utf8),
   *  "predictedSubmissionType": "HOLA" (a GenericData.EnumSymbol),
   *  "confidenceScore": 42.0
   * } (a generic record of schema com.linkedin.messages.standardization.SubmissionTypePrediction)
   * @param something an object to print
   * @return a detailed "json-like" representation of the given object
   */
  public static String detailedPrint(Object something) {
    StringBuilder sb = new StringBuilder();
    detailedPrint(something, sb);
    return sb.toString();
  }

  private static void detailedPrint(Object something, StringBuilder sb) {
    if (something == null) {
      sb.append("null (a real null)");
      return;
    }
    if (something instanceof IndexedRecord) {
      IndexedRecord record = (IndexedRecord) something;
      boolean isSpecific = AvroCompatibilityHelper.isSpecificRecord(record);
      Schema schema = record.getSchema();
      sb.append("{");
      for (Schema.Field field : schema.getFields()) {
        sb.append("\"").append(field.name()).append("\": ");
        detailedPrint(record.get(field.pos()), sb);
        if (field.pos() < schema.getFields().size() - 1) {
          sb.append(", ");
        }
      }
      sb.append("}");
      if (isSpecific) {
        sb.append(" (a specific record of class ").append(something.getClass().getName()).append(")");
      } else {
        sb.append(" (a generic record of schema ").append(schema.getFullName()).append(")");
      }
    } else if (something instanceof Collection) {
      Collection<?> collection = (Collection<?>) something;
      int counter = 0;
      int limit = collection.size() - 1;
      sb.append("[");
      for (Object value : collection) {
        detailedPrint(value, sb);
        if (counter++ < limit) {
          sb.append(", ");
        }
      }
      sb.append("] (a collection of type ").append(something.getClass().getName()).append(")");
    } else if (something instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) something;
      int counter = 0;
      int limit = map.size() - 1;
      sb.append("{");
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        sb.append("\"").append(entry.getKey()).append("\": ");
        detailedPrint(entry.getValue(), sb);
        if (counter++ < limit) {
          sb.append(", ");
        }
      }
      sb.append("} (a map of type ").append(something.getClass().getName()).append(")");
    } else if (something instanceof GenericData.EnumSymbol) {
      GenericData.EnumSymbol symbol = (GenericData.EnumSymbol) something;
      sb.append("\"").append(symbol).append("\" (a GenericData.EnumSymbol)");
    } else if (something instanceof Enum) {
      sb.append("\"").append(something).append("\" (an instance of enum class ")
          .append(something.getClass().getName()).append(")");
    } else if (something instanceof CharSequence) {
      sb.append("\"").append(something).append("\" (a string of type ")
          .append(something.getClass().getName()).append(")");
    } else if (something instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) something;
      sb.append("{\"bytes\": \"");
      for (int i = buffer.position(); i < buffer.limit(); i++) {
        sb.append((char) buffer.get(i));
      }
      sb.append("\"}");
    } else {
      sb.append(something);
    }
  }

  private static void traverseSchema(Schema schema, SchemaVisitor visitor, IdentityHashMap<Object, Boolean> visited) {
    if (visited.put(schema, Boolean.TRUE) != null) {
      return; //been there, done that
    }
    visitor.visitSchema(schema);
    switch (schema.getType()) {
      case UNION:
        for (Schema unionBranch : schema.getTypes()) {
          traverseSchema(unionBranch, visitor, visited);
        }
        return;
      case ARRAY:
        traverseSchema(schema.getElementType(), visitor, visited);
        return;
      case MAP:
        traverseSchema(schema.getValueType(), visitor, visited);
        return;
      case RECORD:
        for (Schema.Field field : schema.getFields()) {
          visitor.visitField(field);
          traverseSchema(field.schema(), visitor, visited);
        }
        break;
      default:
    }
  }

  private static Object getFieldValue(IndexedRecord indexedRecord, Schema.Field field) {
    Schema.Field indexedRecordField = indexedRecord.getSchema().getField(field.name());
    Object fieldData = indexedRecordField == null ? null : indexedRecord.get(indexedRecordField.pos());
    if (fieldData instanceof CharSequence) {
      fieldData = resolveValueOf(field.schema(), fieldData.toString());
    }
    return fieldData;
  }

  private static Object resolveValueOf(Schema schema, String textValue) {
    Schema enumSchema = null;
    if (Schema.Type.ENUM.equals(schema.getType())) {
      enumSchema = schema;
    }

    if (Schema.Type.UNION.equals(schema.getType())) {
      Iterator<Schema> schemaIterator = schema.getTypes().iterator();
      while (schemaIterator.hasNext()) {
        Schema next = schemaIterator.next();

        if (Schema.Type.ENUM.equals(next.getType())) {
          enumSchema = next;
          break;
        }
      }
    }

    if (enumSchema != null) {
      if (enumSchema.hasEnumSymbol(textValue)) {
        return AvroCompatibilityHelper.newEnumSymbol(enumSchema, textValue);
      }
    }

    return textValue;
  }

  // start helper functions for indexedRecordToSpecificRecord
  private static Object interpretFieldData(Object fieldData, Schema fieldSchema) throws AvroRuntimeException {
    try {
      switch (fieldSchema.getType()) {
        case ARRAY:
          return interpretArrayFieldData(fieldData, fieldSchema);
        case MAP:
          return interpretMapFieldData(fieldData, fieldSchema);
        default:
          return interpretScalarFieldData(fieldData, fieldSchema);
      }
    } catch (Exception e) {
      throw new AvroRuntimeException("error interpreting field", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Object> interpretArrayFieldData(Object data, Schema schema) {
    List<Object> array = new ArrayList<>();
    for (Object o : ((Collection<Object>) data)) {
      array.add(interpretFieldData(o, schema.getElementType()));
    }
    return array;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> interpretMapFieldData(Object data, Schema schema) {
    Map<String, Object> map = new HashMap<>();
    for (Map.Entry entry : ((Map<Object, Object>) data).entrySet()) {
      map.put(entry.getKey().toString(), interpretFieldData(entry.getValue(), schema.getValueType()));
    }
    return map;
  }

  private static Object interpretScalarFieldData(Object data, Schema schema) throws Exception {
    Schema fieldSchema = resolveSchema(data, schema);
    Class<?> fieldClass = SpecificData.get().getClass(fieldSchema); //null if no specific class loaded

    if (fieldClass == null) {
      boolean isNamedType = AVRO_NAMED_TYPES.contains(fieldSchema.getType());
      throw new IllegalStateException("unable to find/load specific class for schema "
          + (isNamedType ? fieldSchema.getFullName() : fieldSchema.getName()));
    }

    Object fieldValue;

    Number asNumber;
    long longValue;
    double doubleValue;
    switch (fieldSchema.getType()) {
      case NULL:
        fieldValue = null;
        break;
      case FIXED:
        SpecificFixed fixed = (SpecificFixed) fieldClass.newInstance();
        fixed.bytes(((GenericFixed) data).bytes());
        fieldValue = fixed;
        break;
      case RECORD:
        SpecificRecord innerSpecificRecord = (SpecificRecord) fieldClass.newInstance();
        fieldValue = indexedRecordToSpecificRecord(innerSpecificRecord, (IndexedRecord) data);
        break;
      case ENUM:
        fieldValue = fieldClass.getDeclaredMethod("valueOf", String.class).invoke(null, data.toString());
        break;
      case STRING:
        fieldValue = (data != null) ? data.toString() : null;
        break;
      case INT:
        if (data == null) {
          fieldValue = null;
          break;
        }
        assertIsNumber(data, fieldSchema);
        asNumber = (Number) data;
        //data could be larger than what int can take
        longValue = asNumber.longValue();
        if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
          throw new IllegalArgumentException("cant turn " + data.getClass().getName() + " (" + data + ") into a "
              + fieldSchema.getType() + " because it doesnt fit");
        }
        fieldValue = (int) longValue;
        break;
      case LONG:
        if (data == null) {
          fieldValue = null;
          break;
        }
        assertIsNumber(data, fieldSchema);
        asNumber = (Number) data;
        //long is the widest possible int field so no value checks required
        fieldValue = asNumber.longValue();
        break;
      case FLOAT:
        if (data == null) {
          fieldValue = null;
          break;
        }
        assertIsNumber(data, fieldSchema);
        asNumber = (Number) data;
        //data could be larger than what float can take (unless its infinity ..)
        doubleValue = asNumber.doubleValue();
        if (!Double.isInfinite(doubleValue) && (doubleValue > Float.MAX_VALUE || doubleValue < -Float.MAX_VALUE)) {
          throw new IllegalArgumentException("cant turn " + data.getClass().getName() + " (" + data + ") into a "
              + fieldSchema.getType() + " because it doesnt fit");
        }
        fieldValue = (float) doubleValue;
        break;
      case DOUBLE:
        if (data == null) {
          fieldValue = null;
          break;
        }
        assertIsNumber(data, fieldSchema);
        asNumber = (Number) data;
        fieldValue = asNumber.doubleValue();
        break;
      case BYTES:
        if (data == null) {
          fieldValue = null;
          break;
        }
        if (!(data instanceof ByteBuffer)) {
          throw new IllegalArgumentException("dont know how to turn a " + data.getClass().getName() + " (" + data
              + ") into a " + fieldSchema.getType());
        }
        fieldValue = data;  //avro specific record classes expect binary to mean ByteBuffer (see generated setters)
        break;
      default:
        fieldValue = data;
        break;
    }
    return fieldValue;
  }

  private static Schema resolveSchema(Object data, Schema schema) {
    switch (schema.getType()) {
      case UNION:
        //let avro do its thing
        try {
          int schemaIndex = SpecificData.get().resolveUnion(schema, data);
          return schema.getTypes().get(schemaIndex);
        } catch (AvroRuntimeException e) {
          try {
            //try with aliases
            Schema aliased = resolveUnionSchemaWithAliases(data, schema);
            if (aliased != null) {
              return aliased;
            }
          } catch (Exception failureWithAliases) {
            e.addSuppressed(failureWithAliases);
          }
          //try and get a detailed description of the problematic payload
          String detailedObject;
          try {
            detailedObject = detailedPrint(data);
          } catch (Exception oopsie) {
            e.addSuppressed(oopsie);
            throw e;
          }
          throw new AvroRuntimeException("detailed input: " + detailedObject, e);
        }
      default:
        return schema;
    }
  }

  /**
   * attempt to match a datum to a union branch based ONLY on aliases in union branch types vs the full name
   * of the given datum
   * @param datum concrete instance of something
   * @param unionSchema union in  which to look for a matching schema
   * @return 1st matching union branch, null if none
   */
  private static Schema resolveUnionSchemaWithAliases(Object datum, Schema unionSchema) {
    List<Schema> branches = unionSchema.getTypes();
    String datumFullname = null;

    if (datum instanceof IndexedRecord) {
      //TODO - move this to helper, cover enums/fixed generated by ancient vanilla avro
      datumFullname = ((IndexedRecord) datum).getSchema().getFullName();
    }

    if (datumFullname == null || datumFullname.isEmpty()) {
      //were called only to perform alias matching, which can only be done with a fullname
      return null;
    }

    for (int i = 0; i < branches.size(); i++) {
      Schema branchSchema = branches.get(i);
      //is branch a named type? if so, does it have aliases?
      Schema.Type type = branchSchema.getType();
      if (!AVRO_NAMED_TYPES.contains(type)) {
        continue;
      }
      Set<String> aliases = branchSchema.getAliases();
      if (aliases == null || aliases.isEmpty()) {
        continue;
      }
      if (aliases.contains(datumFullname)) {
        //TODO - check match on type at least ?!
        return branchSchema;
      }
    }

    return null;
  }

  private static void assertIsNumber(Object data, Schema fieldSchema) {
    if (!(Number.class.isAssignableFrom(data.getClass()))) {
      throw new IllegalArgumentException("dont know how to turn a " + data.getClass().getName() + " (" + data
          + ") into a " + fieldSchema.getType());
    }
  }

  // end helper functions for indexedRecordToSpecificRecord
}
