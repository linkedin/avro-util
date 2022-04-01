/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro19.backports;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.avroutil1.compatibility.avro19.Avro19SchemaValidator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.internal.Accessor;


/**
 * this class holds a cache of generic and specific default values for various
 * schema fields. the implementations were taken out of avro 1.7.7 classes and
 * exists here for use with older 1.7.* versions that do not have this functionality
 * {@link org.apache.avro.specific.SpecificData} and
 * {@link org.apache.avro.generic.GenericData}
 */
public class Avro19DefaultValuesCache {

  private static final Map<Schema.Field, Object> GENERIC_CACHED_DEFAULTS = Collections.synchronizedMap(new WeakHashMap<>());
  private static final Map<Schema.Field, Object> SPECIFIC_CACHED_DEFAULTS = Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Gets the default value of the given field, if any.
   * @param field the field whose default value should be retrieved.
   * @return the default value associated with the given field,
   * or null if none is specified in the schema.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static Object getDefaultValue(Schema.Field field, boolean specific) {

    JsonNode json = Accessor.defaultValue(field);
    Schema schema = field.schema();
    if (json == null) {
      throw new AvroRuntimeException("Field " + field + " has no default value");
    }
    if (json.isNull()
        && (schema.getType() == Schema.Type.NULL
        || (schema.getType() == Schema.Type.UNION
        && schema.getTypes().get(0).getType() == Schema.Type.NULL))) {
      return null;
    }

    Map<Schema.Field, Object> cache = specific ? SPECIFIC_CACHED_DEFAULTS : GENERIC_CACHED_DEFAULTS;

    // Check the cache
    Object defaultValue = cache.get(field);
    if (defaultValue != null) {
      return defaultValue;
    }

    //validate the default JsonNode vs the fieldSchema, because old avro doesnt validate
    //and applying the logic below to decode will return very weird results
    if (!Avro19SchemaValidator.isValidDefault(schema, json)) {
      //throw ~the same exception modern avro would
      String message = "Invalid default for field " + field.name() + ": "
          + json + " (a " + json.getClass().getSimpleName() + ") is not a " + schema;
      throw new AvroTypeException(message);
    }

    // If not cached, get the default Java value by encoding the default JSON
    // value and then decoding it (same as avro does in ResolvingGrammarGenerator.resolveRecords()):
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
      ResolvingGrammarGenerator.encode(encoder, schema, json);
      encoder.flush();
      ByteArrayInputStream is = new ByteArrayInputStream(baos.toByteArray());
      BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(is, null);
      DatumReader reader;
      if (specific) {
        reader = new SpecificDatumReader(schema);
      } else {
        reader = new GenericDatumReader(schema);
      }
      defaultValue = reader.read(null, decoder);
      cache.put(field, defaultValue);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }

    return defaultValue;
  }
}
