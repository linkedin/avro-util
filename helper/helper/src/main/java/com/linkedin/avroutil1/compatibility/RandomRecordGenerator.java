/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

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
    return newRandomGeneric(of, config);
  }

  private Object newRandomGeneric(Schema of, RecordGenerationConfig config) {
    Random random = config.random();
    int size;
    byte[] randomBytes;
    int index;
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
        return sb.toString();
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
        GenericData.Record record = new GenericData.Record(of);
        for (Schema.Field field : of.getFields()) {
          //TODO - extend to allow (multiple-hop-long) self-references to complete the experience :-)
          Schema fieldSchema = field.schema();
          Object randomValue = newRandomGeneric(fieldSchema, config);
          record.put(field.pos(), randomValue);
        }
        return record;
      case ARRAY:
        size = random.nextInt(11); //[0, 10]
        GenericData.Array<Object> array = new GenericData.Array<>(size, of);
        Schema elementType = of.getElementType();
        for (int i = 0; i < size; i++) {
          array.add(newRandomGeneric(elementType, config));
        }
        return array;
      case MAP:
        size = random.nextInt(11); //[0, 10]
        HashMap<String, Object> map = new HashMap<>(size);
        Schema valueType = of.getValueType();
        for (int i = 0; i < size; i++) {
          String key = "key-" + i; //TODO - better randomness (yet results should be unique)
          map.put(key, newRandomGeneric(valueType, config));
        }
        return map;
      case UNION:
        List<Schema> branches = of.getTypes();
        index = random.nextInt(branches.size());
        Schema branch = branches.get(index);
        return newRandomGeneric(branch, config);
      default:
        throw new UnsupportedOperationException("unhandled: " + of.getType());
    }
  }
}
