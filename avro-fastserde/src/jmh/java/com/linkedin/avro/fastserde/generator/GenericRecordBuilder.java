//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.linkedin.avro.fastserde.generator;

import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import com.linkedin.avro.fastserde.generator.RecordBuilderBase;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;

public class GenericRecordBuilder extends RecordBuilderBase<Record> {
  private final Record record;

  public GenericRecordBuilder(Schema schema) {
    super(schema, GenericData.get());
    this.record = new Record(schema);
  }

  public Object get(String fieldName) {
    return this.get(this.schema().getField(fieldName));
  }

  public Object get(Field field) {
    return this.get(field.pos());
  }

  protected Object get(int pos) {
    return this.record.get(pos);
  }

  public GenericRecordBuilder set(String fieldName, Object value) {
    return this.set(this.schema().getField(fieldName), value);
  }

  public GenericRecordBuilder set(Field field, Object value) {
    return this.set(field, field.pos(), value);
  }

  protected GenericRecordBuilder set(int pos, Object value) {
    return this.set(this.fields()[pos], pos, value);
  }

  private GenericRecordBuilder set(Field field, int pos, Object value) {
    this.record.put(pos, value);
    this.fieldSetFlags()[pos] = true;
    return this;
  }

  public boolean has(String fieldName) {
    return this.has(this.schema().getField(fieldName));
  }

  public boolean has(Field field) {
    return this.has(field.pos());
  }

  protected boolean has(int pos) {
    return this.fieldSetFlags()[pos];
  }

  public GenericRecordBuilder clear(String fieldName) {
    return this.clear(this.schema().getField(fieldName));
  }

  public GenericRecordBuilder clear(Field field) {
    return this.clear(field.pos());
  }

  protected GenericRecordBuilder clear(int pos) {
    this.record.put(pos, (Object)null);
    this.fieldSetFlags()[pos] = false;
    return this;
  }

  public Record build() {
    Record record;
    try {
      record = new Record(this.schema());
    } catch (Exception var9) {
      throw new AvroRuntimeException(var9);
    }

    Field[] arr$ = this.fields();
    int len$ = arr$.length;

    for(int i$ = 0; i$ < len$; ++i$) {
      Field field = arr$[i$];

      Object value;
      try {
        value = this.getWithDefault(field);
      } catch (IOException var8) {
        throw new AvroRuntimeException(var8);
      }

      if (value != null) {
        record.put(field.pos(), value);
      }
    }

    return record;
  }

  private Object getWithDefault(Field field) throws IOException {
    return this.fieldSetFlags()[field.pos()] ? this.record.get(field.pos()) : this.defaultValue(field);
  }

  public int hashCode() {
    int prime = 1;
    int result = super.hashCode();
    result = 31 * result + (this.record == null ? 0 : this.record.hashCode());
    return result;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!super.equals(obj)) {
      return false;
    } else if (this.getClass() != obj.getClass()) {
      return false;
    } else {
      GenericRecordBuilder other = (GenericRecordBuilder)obj;
      if (this.record == null) {
        if (other.record != null) {
          return false;
        }
      } else if (!this.record.equals(other.record)) {
        return false;
      }

      return true;
    }
  }
}
