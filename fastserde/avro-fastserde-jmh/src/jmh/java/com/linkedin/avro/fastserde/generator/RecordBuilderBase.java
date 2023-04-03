//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.linkedin.avro.fastserde.generator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;

public abstract class RecordBuilderBase<T extends IndexedRecord> implements RecordBuilder<T> {
  private static final Field[] EMPTY_FIELDS = new Field[0];
  private final Schema schema;
  private final Field[] fields;
  private final boolean[] fieldSetFlags;
  private final GenericData data;

  protected final Schema schema() {
    return this.schema;
  }

  protected final Field[] fields() {
    return this.fields;
  }

  protected final boolean[] fieldSetFlags() {
    return this.fieldSetFlags;
  }

  protected final GenericData data() {
    return this.data;
  }

  protected RecordBuilderBase(Schema schema, GenericData data) {
    this.schema = schema;
    this.data = data;
    this.fields = (Field[])((Field[])schema.getFields().toArray(EMPTY_FIELDS));
    this.fieldSetFlags = new boolean[this.fields.length];
  }

  protected RecordBuilderBase(RecordBuilderBase<T> other, GenericData data) {
    this.schema = other.schema;
    this.data = data;
    this.fields = (Field[])((Field[])this.schema.getFields().toArray(EMPTY_FIELDS));
    this.fieldSetFlags = new boolean[other.fieldSetFlags.length];
    System.arraycopy(other.fieldSetFlags, 0, this.fieldSetFlags, 0, this.fieldSetFlags.length);
  }

  protected static boolean isValidValue(Field f, Object value) {
    if (value != null) {
      return true;
    } else {
      Schema schema = f.schema();
      Type type = schema.getType();
      if (type == Type.NULL) {
        return true;
      } else {
        if (type == Type.UNION) {
          Iterator i$ = schema.getTypes().iterator();

          while(i$.hasNext()) {
            Schema s = (Schema)i$.next();
            if (s.getType() == Type.NULL) {
              return true;
            }
          }
        }

        return false;
      }
    }
  }

  protected Object defaultValue(Field field) throws IOException {
    return null;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (this.getClass() != obj.getClass()) {
      return false;
    } else {
      RecordBuilderBase other = (RecordBuilderBase)obj;
      if (!Arrays.equals(this.fieldSetFlags, other.fieldSetFlags)) {
        return false;
      } else {
        if (this.schema == null) {
          if (other.schema != null) {
            return false;
          }
        } else if (!this.schema.equals(other.schema)) {
          return false;
        }

        return true;
      }
    }
  }
}
