package com.linkedin.avroutil1.compatibility.avro14.backports;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.parsing.Avro14ResolvingGrammarGeneratorAccessUtil;
import org.codehaus.jackson.JsonNode;


/**
 * a partial back-port of avro 1.8's {@link org.apache.avro.generic.GenericData} class
 */
public class Avro18GenericData {

  private static final Map<Schema.Field, Object> CACHED_DEFAULTS = Collections.synchronizedMap(new WeakHashMap<>());

  /**
   * Gets the default value of the given field, if any.
   * @param field the field whose default value should be retrieved.
   * @return the default value associated with the given field,
   * or null if none is specified in the schema.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static Object getDefaultValue(Schema.Field field) {
    JsonNode json = field.defaultValue();
    if (json == null)
      throw new AvroRuntimeException("Field " + field
          + " not set and has no default value");
    if (json.isNull()
        && (field.schema().getType() == Schema.Type.NULL
        || (field.schema().getType() == Schema.Type.UNION
        && field.schema().getTypes().get(0).getType() == Schema.Type.NULL))) {
      return null;
    }

    // Check the cache
    Object defaultValue = CACHED_DEFAULTS.get(field);

    // If not cached, get the default Java value by encoding the default JSON
    // value and then decoding it:
    if (defaultValue == null)
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder encoder = new BinaryEncoder(baos);
        Avro14ResolvingGrammarGeneratorAccessUtil.encode(encoder, field.schema(), json);
        encoder.flush();
        BinaryDecoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray(), null);
        //TODO - difference between specific and generic here
        defaultValue = new GenericDatumReader<>(field.schema()).read(null, decoder);

        CACHED_DEFAULTS.put(field, defaultValue);
      } catch (IOException e) {
        throw new AvroRuntimeException(e);
      }

    return defaultValue;
  }
}
