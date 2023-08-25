/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * helper functions for common basic avro encoding/decoding operations
 */
public class AvroCodecUtil {

    public static byte[] serializeBinary(IndexedRecord record) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(os);
        DatumWriter<IndexedRecord> writer = AvroCompatibilityHelper.isSpecificRecord(record) ?
                new SpecificDatumWriter<>(record.getSchema())
                : new GenericDatumWriter<>(record.getSchema());
        writer.write(record, binaryEncoder);
        binaryEncoder.flush();
        os.flush();
        return os.toByteArray();
    }

   /**
    * serializes an IndexedRecord to a json string in Avro's json encoding format (pretty-printed).
    */
    public static String serializeJson(IndexedRecord record, AvroVersion format) throws IOException {
        return serializeJson(record, format, false);
    }

   /**
    * Serialize an IndexedRecord (Generic Record or Specific Record) to json string (in Avro's json encoding format).
    * @param record the record to serialize
    * @param format the version of avro to use
    * @param oneline whether to output the json in one line or pretty printed.
    */
    public static String serializeJson(IndexedRecord record, AvroVersion format, boolean oneline) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder encoder = AvroCompatibilityHelper.newJsonEncoder(record.getSchema(), os, !oneline, format);
        DatumWriter<IndexedRecord> writer = AvroCompatibilityHelper.isSpecificRecord(record) ?
                new SpecificDatumWriter<>(record.getSchema())
                : new GenericDatumWriter<>(record.getSchema());
        writer.write(record, encoder);
        encoder.flush();
        os.flush();
        //java 10+ has a more efficient impl in os, but we want to remain java 8 compatible
        //noinspection StringOperationCanBeSimplified
        return new String(os.toByteArray(), StandardCharsets.UTF_8);
    }

    public static GenericRecord deserializeAsGeneric(byte[] binarySerialized, Schema writerSchema, Schema readerSchema) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(binarySerialized);
        BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(is, false, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
        GenericRecord result = reader.read(null, decoder);
        //make sure everything was read out
        if (is.available() != 0) {
            throw new IllegalStateException("leftover bytes in input. schema given likely partial?");
        }
        return result;
    }

    public static GenericRecord deserializeAsGeneric(String jsonSerialized, Schema writerSchema, Schema readerSchema) throws IOException {
        InputStream is = new ByteArrayInputStream(jsonSerialized.getBytes(StandardCharsets.UTF_8));
        Decoder decoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(writerSchema, is);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
        GenericRecord result = reader.read(null, decoder);
        //make sure everything was read out
        if (bytesLeftoverJson(is)) {
            throw new IllegalStateException("leftover bytes in input. schema given likely partial?");
        }
        return result;
    }

    /**
     * Check if bytes leftover in the inputStream after reading by Json parser.
     * Json parser may leave the '}' in inputStream, so single '}' in the inputStream should be ignored.
     * ' ', '\t', and '\n' should be ignored.
     */
    public static boolean bytesLeftoverJson(InputStream inputStream) throws IOException {
        if (inputStream.available() == 0) {
            return false;
        }

        Set<Character> skippedCharacters = new HashSet<>(Arrays.asList(' ', '\t', '\n'));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        boolean rightBracefound = false;

        int r;
        while ((r = bufferedReader.read()) != -1) {
            char ch = (char) r;
            if (ch == '}') {
                if (rightBracefound) {
                    return true;
                }
                rightBracefound = true;
            } else if (!skippedCharacters.contains(ch)) {
                return true;
            }
        }
        return false;
    }

    public static <T> T deserializeAsSpecific(byte[] binarySerialized, Schema writerSchema, Class<T> specificRecordClass) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(binarySerialized);
        BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(is, false, null);
        Schema readerSchema = SpecificData.get().getSchema(specificRecordClass);
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
        T result = reader.read(null, decoder);
        //make sure everything was read out
        if (is.available() != 0) {
            throw new IllegalStateException("leftover bytes in input. schema given likely partial?");
        }
        return result;
    }

    public static <T> T deserializeAsSpecificWithAliases(byte[] binarySerialized, Schema writerSchema, Class<T> specificRecordClass) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(binarySerialized);
        BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(is, false, null);
        SpecificDatumReader<T> reader = AvroCompatibilityHelper.newAliasAwareSpecificDatumReader(writerSchema, specificRecordClass);
        T result = reader.read(null, decoder);
        //make sure everything was read out
        if (is.available() != 0) {
            throw new IllegalStateException("leftover bytes in input. schema given likely partial?");
        }
        return result;
    }

    public static <T> T deserializeAsSpecific(String jsonSerialized, Schema writerSchema, Class<T> specificRecordClass) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(jsonSerialized.getBytes(StandardCharsets.UTF_8));
        Decoder decoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(writerSchema, is);
        Schema readerSchema = SpecificData.get().getSchema(specificRecordClass);
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(writerSchema, readerSchema);
        T result = reader.read(null, decoder);
        //make sure everything was read out
        if (is.available() != 0) {
            throw new IllegalStateException("leftover bytes in input. schema given likely partial?");
        }
        return result;
    }

    public static <T> T deserializeAsSpecificWithAliases(String jsonSerialized, Schema writerSchema, Class<T> specificRecordClass) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(jsonSerialized.getBytes(StandardCharsets.UTF_8));
        Decoder decoder = AvroCompatibilityHelper.newCompatibleJsonDecoder(writerSchema, is);
        SpecificDatumReader<T> reader = AvroCompatibilityHelper.newAliasAwareSpecificDatumReader(writerSchema, specificRecordClass);
        T result = reader.read(null, decoder);
        //make sure everything was read out
        if (is.available() != 0) {
            throw new IllegalStateException("leftover bytes in input. schema given likely partial?");
        }
        return result;
    }
}
