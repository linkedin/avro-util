package com.linkedin.avro.api;

import java.nio.ByteBuffer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * An extension to the {@link GenericRecord} API that includes utility functions for
 * casting fields to the right type, including both boxed and unboxed. The following
 * is worth keeping in mind:
 *
 * - All getter and setter functions can throw {@link ClassCastException} if used on
 *   the wrong type.
 * - All setter functions can throw {@link AvroRuntimeException} if used on a field
 *   which doesn't exist in the schema, which is the same behavior as
 *   {@link GenericData.Record}.
 * - All primitive getter functions likewise can throw {@link AvroRuntimeException}
 *   if used on a field which doesn't exist in the schema.
 * - All object getter functions can return null if used on a field which doesn't
 *   exist in the schema, which is the same behavior as {@link GenericData.Record#get()}
 * - All primitive getter functions can throw a {@link NullPointerException} if used to
 *   retrieved an optional field for which the value has been set to null. In order
 *   to avoid this, one can use the {@link #isNotNull(String)} function before calling
 *   a primitive getter.
 */
public interface PrimitiveGenericRecord extends GenericRecord {
  /**
   * A function to check if a given field contains a non-null value. This can be used
   * in conjunction with the primitive getters to avoid boxing. For example, with the
   * regular {@link GenericRecord} API, it is common to do the following:
   *
   * Float myField = record.get("myField");
   * if (myField != null) {
   *   doSomething(myField);
   * }
   *
   * Instead, with the {@link PrimitiveGenericRecord} API, the above pattern would be
   * refactored into:
   *
   * if (record.isNotNull("myField")) {
   *   doSomething(record.getPrimitiveFloat("myField"));
   * }
   *
   * This allows optional fields to be handled as primitives and thus avoid the boxing
   * overhead.
   *
   * @param key the field name
   * @return true if the field has a value defined, or false if it is null
   * @throws AvroRuntimeException
   */
  boolean isNotNull(String key) throws AvroRuntimeException;

  // Boolean getter/setters
  Boolean getBoolean(String key) throws ClassCastException;
  boolean getPrimitiveBoolean(String key) throws AvroRuntimeException, ClassCastException, NullPointerException;
  void putBoolean(String key, Boolean value) throws AvroRuntimeException, ClassCastException;
  void putPrimitiveBoolean(String key, boolean value) throws AvroRuntimeException, ClassCastException;

  // Bytes getter/setter
  ByteBuffer getBytes(String key) throws ClassCastException;
  void putByteBuffer(String key, ByteBuffer value) throws AvroRuntimeException, ClassCastException;

  // Double getter/setters
  Double getDouble(String key) throws ClassCastException;
  double getPrimitiveDouble(String key) throws AvroRuntimeException, ClassCastException, NullPointerException;
  void putDouble(String key, Double value) throws AvroRuntimeException, ClassCastException;
  void putPrimitiveDouble(String key, double value) throws AvroRuntimeException, ClassCastException;

  // Float getter/setters
  Float getFloat(String key) throws ClassCastException;
  float getPrimitiveFloat(String key) throws AvroRuntimeException, ClassCastException, NullPointerException;
  void putFloat(String key, Float value) throws AvroRuntimeException, ClassCastException;
  void putPrimitiveFloat(String key, float value) throws AvroRuntimeException, ClassCastException;

  // Integer getter/setters
  Integer getInteger(String key) throws ClassCastException;
  int getPrimitiveInteger(String key) throws AvroRuntimeException, ClassCastException, NullPointerException;
  void putInteger(String key, Integer value) throws AvroRuntimeException, ClassCastException;
  void putPrimitiveInteger(String key, int value) throws AvroRuntimeException, ClassCastException;

  // Long getter/setters
  Long getLong(String key) throws ClassCastException;
  long getPrimitiveLong(String key) throws AvroRuntimeException, ClassCastException, NullPointerException;
  void putLong(String key, Long value) throws AvroRuntimeException, ClassCastException;
  void putPrimitiveLong(String key, long value) throws AvroRuntimeException, ClassCastException;

  // String getter/setters
  CharSequence getCharSequence(String key) throws ClassCastException;
  void putCharSequence(String key, CharSequence value) throws AvroRuntimeException, ClassCastException;
  String getString(String key) throws ClassCastException;
  void putString(String key, String value) throws AvroRuntimeException, ClassCastException;
  Utf8 getUtf8(String key) throws ClassCastException;
  void putUtf8(String key, Utf8 value) throws AvroRuntimeException, ClassCastException;

  // Record getter/setters
  PrimitiveGenericRecord getRecord(String key) throws ClassCastException;
  void putRecord(String key, PrimitiveGenericRecord value) throws AvroRuntimeException, ClassCastException;

  // TODO: Add getter/setters for maps and arrays of all the above types...?
}
