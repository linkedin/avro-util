package com.linkedin.avro.fastserde;

import org.apache.avro.generic.CustomizedGenericDatumReader;
import org.apache.avro.generic.CustomizedGenericDatumReaderForAvro14;
import org.apache.avro.generic.CustomizedGenericDatumWriter;
import org.apache.avro.generic.CustomizedSpecificDatumReader;
import org.apache.avro.generic.CustomizedSpecificDatumReaderForAvro14;
import org.apache.avro.generic.CustomizedSpecificDatumWriter;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;


/**
 * This utility class includes several vanilla Avro based {@link FastDeserializer} and {@link FastSerializer} for the following scenarios:
 * 1. Before fast class generation is completed, and the class defined here will be used with flag: runtimeClassGenerationDone to be `false`.
 * 2. The same set of class will be used with flag: runtimeClassGenerationDone to be `true` when class generation fails.
 * For #1, Fast Datum Reader/Writer will keep polling fast classes and for #2, it will stop polling since it will
 * know the fast class generation fails, and it is useless to keep polling.
 */
public class FastSerdeUtils {
  public static class FastDeserializerWithAvroSpecificImpl<V> implements FastDeserializer<V> {
    private final SpecificDatumReader<V> customizedDatumReader;
    private final DatumReaderCustomization customization;
    private final boolean failFast;
    private final boolean runtimeClassGenerationDone;

    public FastDeserializerWithAvroSpecificImpl(Schema writerSchema, Schema readerSchema, SpecificData modelData,
        boolean runtimeClassGenerationDone) {
      this(writerSchema, readerSchema, modelData, null, false, runtimeClassGenerationDone);
    }

    public FastDeserializerWithAvroSpecificImpl(Schema writerSchema, Schema readerSchema, SpecificData modelData
        , DatumReaderCustomization customization, boolean runtimeClassGenerationDone) {
      this(writerSchema, readerSchema, modelData, customization, false, runtimeClassGenerationDone);
    }

    public FastDeserializerWithAvroSpecificImpl(Schema writerSchema, Schema readerSchema,
        SpecificData modelData, DatumReaderCustomization customization, boolean failFast, boolean runtimeClassGenerationDone) {
      this.customization = customization == null ? DatumReaderCustomization.DEFAULT_DATUM_READER_CUSTOMIZATION : customization;
      this.customizedDatumReader = Utils.isAvro14() ?
          new CustomizedSpecificDatumReaderForAvro14<>(writerSchema, readerSchema, this.customization) :
          new CustomizedSpecificDatumReader<>(writerSchema, readerSchema, modelData, this.customization);
      this.failFast = failFast;
      this.runtimeClassGenerationDone = runtimeClassGenerationDone;
    }

    @Override
    public V deserialize(V reuse, Decoder d, DatumReaderCustomization customization) throws IOException {
      if (failFast) {
        throw new UnsupportedOperationException("Fast specific deserializer could not be generated.");
      }
      if (this.customization != customization) {
        throw new IllegalArgumentException(
            "The provided 'customization' doesn't equal to the param passed in the constructor");
      }
      return this.customizedDatumReader.read(reuse, d);
    }


    @Override
    public boolean hasDynamicClassGenerationDone() {
      return runtimeClassGenerationDone;
    }

    @Override
    public boolean isBackedByGeneratedClass() {
      return false;
    }
  }

  public static class FastDeserializerWithAvroGenericImpl<V> implements FastDeserializer<V> {
    private final GenericDatumReader<V> customizedDatumReader;
    private final DatumReaderCustomization customization;
    private final boolean failFast;
    private final boolean runtimeClassGenerationDone;

    public FastDeserializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema,
        GenericData modelData, boolean runtimeClassGenerationDone) {
      this(writerSchema, readerSchema, modelData, null, false, runtimeClassGenerationDone);
    }

    public FastDeserializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema, GenericData modelData,
        DatumReaderCustomization customization, boolean runtimeClassGenerationDone) {
      this(writerSchema, readerSchema, modelData, customization, false, runtimeClassGenerationDone);
    }

    public FastDeserializerWithAvroGenericImpl(Schema writerSchema, Schema readerSchema,
        GenericData modelData, DatumReaderCustomization customization, boolean failFast, boolean runtimeClassGenerationDone) {
      this.customization = customization == null ? DatumReaderCustomization.DEFAULT_DATUM_READER_CUSTOMIZATION : customization;
      this.customizedDatumReader = Utils.isAvro14() ?
          new CustomizedGenericDatumReaderForAvro14<>(writerSchema, readerSchema, this.customization) :
          new CustomizedGenericDatumReader<>(writerSchema, readerSchema, modelData, this.customization);

      this.failFast = failFast;
      this.runtimeClassGenerationDone = runtimeClassGenerationDone;
    }

    @Override
    public V deserialize(V reuse, Decoder d, DatumReaderCustomization customization) throws IOException {
      if (failFast) {
        throw new UnsupportedOperationException("Fast generic deserializer could not be generated.");
      }
      if (this.customization != customization) {
        throw new IllegalArgumentException("The provided 'customization' doesn't equal to the param passed in the constructor");
      }
      return this.customizedDatumReader.read(reuse, d);
    }

    @Override
    public boolean hasDynamicClassGenerationDone() {
      return runtimeClassGenerationDone;
    }

    @Override
    public boolean isBackedByGeneratedClass() {
      return false;
    }
  }

  public static class FastSerializerWithAvroSpecificImpl<V> implements FastSerializer<V> {
    private final CustomizedSpecificDatumWriter<V> customizedDatumWriter;
    private final DatumWriterCustomization customization;
    private final boolean failFast;
    private final boolean runtimeClassGenerationDone;

    public FastSerializerWithAvroSpecificImpl(Schema schema, SpecificData modelData,
        DatumWriterCustomization customization, boolean runtimeClassGenerationDone) {
      this(schema, modelData, customization, false, runtimeClassGenerationDone);
    }

    public FastSerializerWithAvroSpecificImpl(Schema schema, SpecificData modelData,
        DatumWriterCustomization customization, boolean failFast, boolean runtimeClassGenerationDone) {
      modelData = modelData != null ? modelData : SpecificData.get();
      this.customization = customization == null ? DatumWriterCustomization.DEFAULT_DATUM_WRITER_CUSTOMIZATION : customization;
      this.customizedDatumWriter = new CustomizedSpecificDatumWriter<>(schema, modelData, this.customization);
      this.failFast = failFast;
      this.runtimeClassGenerationDone = runtimeClassGenerationDone;
    }

    @Override
    public void serialize(V data, Encoder e, DatumWriterCustomization customization) throws IOException {
      if (failFast) {
        throw new UnsupportedOperationException("Fast specific serializer could not be generated.");
      }

      if (this.customization != customization) {
        throw new IllegalArgumentException("The provided 'customization' doesn't equal to the param passed in the constructor");
      }
      customizedDatumWriter.write(data, e);
    }

    @Override
    public boolean hasDynamicClassGenerationDone() {
      return runtimeClassGenerationDone;
    }

    @Override
    public boolean isBackedByGeneratedClass() {
      return false;
    }
  }

  public static class FastSerializerWithAvroGenericImpl<V> implements FastSerializer<V> {
    private final CustomizedGenericDatumWriter<V> customizedDatumWriter;
    private final DatumWriterCustomization customization;
    private final boolean failFast;
    private final boolean runtimeClassGenerationDone;

    public FastSerializerWithAvroGenericImpl(Schema schema, GenericData modelData,
        DatumWriterCustomization customization, boolean runtimeClassGenerationDone) {
      this(schema, modelData, customization, false, runtimeClassGenerationDone);
    }

    public FastSerializerWithAvroGenericImpl(Schema schema, GenericData modelData,
        DatumWriterCustomization customization, boolean failFast, boolean runtimeClassGenerationDone) {
      modelData = modelData != null ? modelData : GenericData.get();
      this.customization = customization == null ? DatumWriterCustomization.DEFAULT_DATUM_WRITER_CUSTOMIZATION : customization;
      this.customizedDatumWriter = new CustomizedGenericDatumWriter<>(schema, modelData, this.customization);
      this.failFast = failFast;
      this.runtimeClassGenerationDone = runtimeClassGenerationDone;
    }

    @Override
    public void serialize(V data, Encoder e, DatumWriterCustomization customization) throws IOException {
      if (failFast) {
        throw new UnsupportedOperationException("Fast generic serializer could not be generated.");
      }
      if (this.customization != customization) {
        throw new IllegalArgumentException("The provided 'customization' doesn't equal to the param passed in the constructor");
      }
      customizedDatumWriter.write(data, e);
    }

    @Override
    public boolean hasDynamicClassGenerationDone() {
      return runtimeClassGenerationDone;
    }

    @Override
    public boolean isBackedByGeneratedClass() {
      return false;
    }

  }
}
