/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro15.backports;

import com.linkedin.avroutil1.compatibility.avro15.codec.CachedResolvingDecoder;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;

import java.io.IOException;


/**
 * this class allows constructing a {@link GenericDatumReader} with
 * a specified {@link GenericData} instance under avro 1.5
 * @param <T>
 */
public class GenericDatumReaderExt<T> extends GenericDatumReader<T> {

  private Schema writer;
  private Schema reader;

  public GenericDatumReaderExt(Schema writer, Schema reader, GenericData genericData) {
    super(writer, reader, genericData);
    this.writer = writer;
    this.reader = reader;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setExpected(Schema reader) throws IOException {
    super.setExpected(reader);
    this.reader = reader;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSchema(Schema writer) {
    super.setSchema(writer);
    this.writer = writer;
    if (reader == null) {
      this.reader = writer;
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public T read(T reuse, Decoder in) throws IOException {
    CachedResolvingDecoder resolver = new CachedResolvingDecoder(Schema.applyAliases(writer, reader), reader, in);
    resolver.init(in);
    T result = (T) read(reuse, reader, resolver);
    resolver.drain();
    return result;
  }

  private Object read(Object old, Schema expected,
                      CachedResolvingDecoder in) throws IOException {
    switch (expected.getType()) {
      case RECORD:
        return readRecord(old, expected, in);
      case ENUM:
        return readEnum(expected, in);
      case ARRAY:
        return readArray(old, expected, in);
      case MAP:
        return readMap(old, expected, in);
      case UNION:
        return read(old, expected.getTypes().get(in.readIndex()), in);
      case FIXED:
        return readFixed(old, expected, in);
      case STRING:
        return readString(old, expected, in);
      case BYTES:
        return readBytes(old, in);
      case INT:
        return readInt(old, expected, in);
      case LONG:
        return in.readLong();
      case FLOAT:
        return in.readFloat();
      case DOUBLE:
        return in.readDouble();
      case BOOLEAN:
        return in.readBoolean();
      case NULL:
        in.readNull();
        return null;
      default:
        throw new AvroRuntimeException("Unknown type: " + expected);
    }
  }

  private Object readRecord(Object old, Schema expected,
                            CachedResolvingDecoder in) throws IOException {
    Object record = newRecord(old, expected);
    final GenericData data = getData();

    for (Schema.Field f : in.readFieldOrder()) {
      int pos = f.pos();
      String name = f.name();
      Object oldDatum = (old != null) ? data.getField(record, name, pos) : null;
      data.setField(record, name, pos, read(oldDatum, f.schema(), in));
    }

    return record;
  }

  private Object readArray(Object old, Schema expected,
                           CachedResolvingDecoder in) throws IOException {
    Schema expectedType = expected.getElementType();
    long l = in.readArrayStart();
    long base = 0;
    if (l > 0) {
      Object array = newArray(old, (int) l, expected);
      do {
        for (long i = 0; i < l; i++) {
          addToArray(array, base + i, read(peekArray(array), expectedType, in));
        }
        base += l;
      } while ((l = in.arrayNext()) > 0);
      return array;
    } else {
      return newArray(old, 0, expected);
    }
  }

  private Object readMap(Object old, Schema expected,
                         CachedResolvingDecoder in) throws IOException {
    Schema eValue = expected.getValueType();
    long l = in.readMapStart();
    Object map = newMap(old, (int) l);
    if (l > 0) {
      do {
        for (int i = 0; i < l; i++) {
          addToMap(map, readString(null, in), read(null, eValue, in));
        }
      } while ((l = in.mapNext()) > 0);
    }
    return map;
  }
}
