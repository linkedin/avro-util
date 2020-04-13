package com.linkedin.avro.api.impl;

import com.linkedin.avro.api.PrimitiveGenericRecord;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;


public class PrimitiveGenericRecordImpl implements PrimitiveGenericRecord {
  private final Schema schema;

  /**
   * This {@link BitSet} is overloaded to hold two types of data:
   *
   * 1. From index 0 to {@link #schema}.getFields().size() -1, a bit representing whether the field at
   *    this position is set (true) or null (false)
   * 2. From {@link #schema}.getFields().size() to the end, a bit representing the value of a given
   *    boolean field.
   */
  private BitSet booleanFlagsAndValues;
  private Object[] objects = null;
  private double[] doubles = null;
  private float[] floats = null;
  private int[] integers = null;
  private long[] longs = null;

  PrimitiveGenericRecordImpl(Schema schema) {
    this.schema = schema;

    List<Schema.Field> fields = schema.getFields();
    int objectIndex = 0,
        booleanIndex = 0,
        doubleIndex = 0,
        floatIndex = 0,
        integerIndex = 0,
        longIndex = 0,
        currentPrimitiveIndex = 0;
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);
      switch (field.schema().getType()) {
        // Objects
        case MAP:
        case ARRAY:
        case RECORD:
        case BYTES:
        case FIXED:
        case STRING:
        case ENUM:
          currentPrimitiveIndex = objectIndex++;
          break;

        // Primitives
        case BOOLEAN: currentPrimitiveIndex = booleanIndex++; break;
        case DOUBLE: currentPrimitiveIndex = doubleIndex++; break;
        case FLOAT: currentPrimitiveIndex = floatIndex++; break;
        case INT: currentPrimitiveIndex = integerIndex++; break;
        case LONG: currentPrimitiveIndex = longIndex++; break;

        case UNION:
          List<Schema> unionTypes = field.schema().getTypes();

          Schema.Type type = null;
          int nonNullTypesFound = 0;
          for (int j = 0; j < unionTypes.size(); j++) {
            type = unionTypes.get(j).getType();

            if (type != Schema.Type.NULL) {
              nonNullTypesFound++;
            }
          }
          if (nonNullTypesFound > 1) {
            // TODO: Maybe support? (probably not worth it...)
            throw new RuntimeException("Enums with more than a single non-null type are not supported.");
          }

          if (type != null) {
            switch (type) {
              // Objects
              case MAP:
              case ARRAY:
              case RECORD:
              case BYTES:
              case FIXED:
              case STRING:
              case ENUM:
                currentPrimitiveIndex = objectIndex++;
                break;

              // Primitives
              case BOOLEAN: currentPrimitiveIndex = booleanIndex++; break;
              case DOUBLE: currentPrimitiveIndex = doubleIndex++; break;
              case FLOAT: currentPrimitiveIndex = floatIndex++; break;
              case INT: currentPrimitiveIndex = integerIndex++; break;
              case LONG: currentPrimitiveIndex = longIndex++; break;
            }
          }
          break;
        case NULL:
          // No need to store this guy in any of the dedicated arrays, if it's always going to be null...
          break;
        default:
          throw new RuntimeException("You should have never made it here!");
      }
      if (!(field instanceof PrimitiveField)) {
        /**
         * Theoretically, this should not be allowed since the list of fields is supposed to be a
         * {@link org.apache.avro.Schema.LockableArrayList}, but it looks like Doug forgot to override
         * the {@link List#set(int, Object)} function so it apparently can work...
         */
        fields.set(i, new PrimitiveField(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order(), currentPrimitiveIndex));
      }
    }
    if (objectIndex > 0) {
      this.objects = new Object[objectIndex];
    }
    if (doubleIndex > 0) {
      this.doubles = new double[doubleIndex];
    }
    if (floatIndex > 0) {
      this.floats = new float[floatIndex];
    }
    if (integerIndex > 0) {
      this.integers = new int[integerIndex];
    }
    if (longIndex > 0) {
      this.longs = new long[longIndex];
    }

    // At construction-time, all bits are false (i.e. all fields are null)
    this.booleanFlagsAndValues = new BitSet(fields.size() + booleanIndex);
  }

  @Override
  public boolean isNotNull(String key) throws AvroRuntimeException {
    return booleanFlagsAndValues.get(schema.getField(key).pos());
  }

  @Override
  public Boolean getBoolean(String key) throws ClassCastException {
    if (isNotNull(key)) {
      return getPrimitiveBoolean(key);
    }
    return null;
  }

  @Override
  public boolean getPrimitiveBoolean(String key) throws AvroRuntimeException, ClassCastException, NullPointerException {
    return booleanFlagsAndValues.get(schema.getFields().size() + getPrimitiveIndex(key));
  }

  @Override
  public void putBoolean(String key, Boolean value) throws AvroRuntimeException, ClassCastException {
    if (null == value) {
      booleanFlagsAndValues.clear(schema.getField(key).pos());
      return;
    }
    putPrimitiveBoolean(key, value);
  }

  @Override
  public void putPrimitiveBoolean(String key, boolean value) throws AvroRuntimeException, ClassCastException {
    booleanFlagsAndValues.set(schema.getFields().size() + getPrimitiveIndex(key), value);
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public ByteBuffer getBytes(String key) throws ClassCastException {
    return (ByteBuffer) get(key);
  }

  @Override
  public void putByteBuffer(String key, ByteBuffer value) throws AvroRuntimeException, ClassCastException {
    put(key, value);
  }

  @Override
  public Double getDouble(String key) throws ClassCastException {
    if (isNotNull(key)) {
      return getPrimitiveDouble(key);
    }
    return null;
  }

  @Override
  public double getPrimitiveDouble(String key) throws AvroRuntimeException, ClassCastException, NullPointerException {
    return doubles[getPrimitiveIndex(key)];
  }

  @Override
  public void putDouble(String key, Double value) throws AvroRuntimeException, ClassCastException {
    if (null == value) {
      booleanFlagsAndValues.clear(schema.getField(key).pos());
      return;
    }
    putPrimitiveDouble(key, value);
  }

  @Override
  public void putPrimitiveDouble(String key, double value) throws AvroRuntimeException, ClassCastException {
    doubles[getPrimitiveIndex(key)] = value;
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public Float getFloat(String key) throws ClassCastException {
    if (isNotNull(key)) {
      return getPrimitiveFloat(key);
    }
    return null;
  }

  @Override
  public float getPrimitiveFloat(String key) throws AvroRuntimeException, ClassCastException, NullPointerException {
    return floats[getPrimitiveIndex(key)];
  }

  @Override
  public void putFloat(String key, Float value) throws AvroRuntimeException, ClassCastException {
    if (null == value) {
      booleanFlagsAndValues.clear(schema.getField(key).pos());
      return;
    }
    putPrimitiveFloat(key, value);
  }

  @Override
  public void putPrimitiveFloat(String key, float value) throws AvroRuntimeException, ClassCastException {
    floats[getPrimitiveIndex(key)] = value;
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public Integer getInteger(String key) throws ClassCastException {
    if (isNotNull(key)) {
      return getPrimitiveInteger(key);
    }
    return null;
  }

  @Override
  public int getPrimitiveInteger(String key) throws AvroRuntimeException, ClassCastException, NullPointerException {
    return integers[getPrimitiveIndex(key)];
  }

  @Override
  public void putInteger(String key, Integer value) throws AvroRuntimeException, ClassCastException {
    if (null == value) {
      booleanFlagsAndValues.clear(schema.getField(key).pos());
      return;
    }
    putPrimitiveInteger(key, value);
  }

  @Override
  public void putPrimitiveInteger(String key, int value) throws AvroRuntimeException, ClassCastException {
    integers[getPrimitiveIndex(key)] = value;
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public Long getLong(String key) throws ClassCastException {
    if (isNotNull(key)) {
      return getPrimitiveLong(key);
    }
    return null;
  }

  @Override
  public long getPrimitiveLong(String key) throws AvroRuntimeException, ClassCastException, NullPointerException {
    return longs[getPrimitiveIndex(key)];
  }

  @Override
  public void putLong(String key, Long value) throws AvroRuntimeException, ClassCastException {
    if (null == value) {
      booleanFlagsAndValues.clear(schema.getField(key).pos());
      return;
    }
    putPrimitiveLong(key, value);
  }

  @Override
  public void putPrimitiveLong(String key, long value) throws AvroRuntimeException, ClassCastException {
    longs[getPrimitiveIndex(key)] = value;
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public CharSequence getCharSequence(String key) throws ClassCastException {
    return (CharSequence) get(key);
  }

  @Override
  public void putCharSequence(String key, CharSequence value) throws AvroRuntimeException, ClassCastException {
    put(key, value);
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public String getString(String key) throws ClassCastException {
    return (String) get(key);
  }

  @Override
  public void putString(String key, String value) throws AvroRuntimeException, ClassCastException {
    put(key, value);
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public Utf8 getUtf8(String key) throws ClassCastException {
    return (Utf8) get(key);
  }

  @Override
  public void putUtf8(String key, Utf8 value) throws AvroRuntimeException, ClassCastException {
    put(key, value);
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public PrimitiveGenericRecord getRecord(String key) throws ClassCastException {
    return (PrimitiveGenericRecord) get(key);
  }

  @Override
  public void putRecord(String key, PrimitiveGenericRecord value) throws AvroRuntimeException, ClassCastException {
    put(key, value);
    booleanFlagsAndValues.set(schema.getField(key).pos());
  }

  @Override
  public void put(String key, Object v) {
    put((PrimitiveField) schema.getField(key), v);
  }

  @Override
  public Object get(String key) {
    return get((PrimitiveField) schema.getField(key));
  }

  @Override
  public void put(int i, Object v) {
    put((PrimitiveField) schema.getFields().get(i), v);
  }

  @Override
  public Object get(int i) {
    return get((PrimitiveField) schema.getFields().get(i));
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  private void put(PrimitiveField field, Object v) {
    switch (field.schema().getType()) {
      // We assume the primitive types will be set using their respective primitive setters,
      // so the common case for calling this function should be to set objects, hence we look
      // for those cases first.
      case MAP:
      case ARRAY:
      case RECORD:
      case BYTES:
      case FIXED:
      case STRING:
      case ENUM:
        objects[field.getPrimitivePosition()] = v;
        break;

      // Now on to the primitives...
      case BOOLEAN: putBoolean(field.name(), (boolean) v); break;
      case DOUBLE: putDouble(field.name(), (double) v); break;
      case FLOAT: putFloat(field.name(), (float) v); break;
      case INT: putInteger(field.name(), (int) v); break;
      case LONG: putLong(field.name(), (long) v); break;

      case UNION:
        if (null == v) {
          booleanFlagsAndValues.clear(field.pos());
        }

        // Can be assumed to contain at most two types, thanks to the construction-time check.
        List<Schema> unionTypes = field.schema().getTypes();
        Schema.Type type = null;
        for (int j = 0; j < unionTypes.size(); j++) {
          type = unionTypes.get(j).getType();
          if (type != Schema.Type.NULL) {
            break;
          }
        }
        if (type != null) {
          switch (type) {
            // Objects
            case MAP:
            case ARRAY:
            case RECORD:
            case BYTES:
            case FIXED:
            case STRING:
            case ENUM:
              objects[field.getPrimitivePosition()] = v;
              break;

            // Now on to the primitives...
            case BOOLEAN: putBoolean(field.name(), (boolean) v); break;
            case DOUBLE: putDouble(field.name(), (double) v); break;
            case FLOAT: putFloat(field.name(), (float) v); break;
            case INT: putInteger(field.name(), (int) v); break;
            case LONG: putLong(field.name(), (long) v); break;
            default:
              throw new RuntimeException("You should have never made it here!");
          }
        }

        break;

      case NULL:
        // Nothing to do for fields that are always null...
        break;
      default:
        throw new RuntimeException("You should have never made it here!");
    }
  }

  private Object get(PrimitiveField field) {
    switch (field.schema().getType()) {
      // We assume the primitive types will be set using their respective primitive setters,
      // so the common case for calling this function should be to set objects, hence we look
      // for those cases first.
      case MAP:
      case ARRAY:
      case RECORD:
      case BYTES:
      case FIXED:
      case STRING:
      case ENUM:
        return objects[field.getPrimitivePosition()];

        // Now on to the primitives...
      case BOOLEAN: return getPrimitiveBoolean(field.name());
      case DOUBLE: return getPrimitiveDouble(field.name());
      case FLOAT: return getPrimitiveFloat(field.name());
      case INT: return getPrimitiveInteger(field.name());
      case LONG: return getPrimitiveLong(field.name());

      case UNION:
        if (!booleanFlagsAndValues.get(field.pos())) {
          return null;
        }

        // Can be assumed to contain at most two types, thanks to the construction-time check.
        List<Schema> unionTypes = field.schema().getTypes();
        Schema.Type type = null;
        for (int j = 0; j < unionTypes.size(); j++) {
          type = unionTypes.get(j).getType();
          if (type != Schema.Type.NULL) {
            break;
          }
        }
        if (type != null) {
          switch (type) {
            // Objects
            case MAP:
            case ARRAY:
            case RECORD:
            case BYTES:
            case FIXED:
            case STRING:
            case ENUM:
              return objects[field.getPrimitivePosition()];

            // Now on to the primitives...
            case BOOLEAN: return getPrimitiveBoolean(field.name());
            case DOUBLE: return getPrimitiveDouble(field.name());
            case FLOAT: return getPrimitiveFloat(field.name());
            case INT: return getPrimitiveInteger(field.name());
            case LONG: return getPrimitiveLong(field.name());

            case NULL: return null;
            default:
              throw new RuntimeException("You should have never made it here!");
          }
        } else {
          throw new RuntimeException("You should have never made it here!");
        }

      default:
        throw new RuntimeException("You should have never made it here!");
    }
  }

  private int getPrimitiveIndex(String key) {
    return ((PrimitiveField)schema.getField(key)).getPrimitivePosition();
  }
}
