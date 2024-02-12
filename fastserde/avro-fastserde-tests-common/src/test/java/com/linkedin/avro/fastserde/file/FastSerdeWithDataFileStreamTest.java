package com.linkedin.avro.fastserde.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.avro.fastserde.FastGenericDatumReader;
import com.linkedin.avro.fastserde.FastSpecificDatumReader;
import com.linkedin.avro.fastserde.FastSpecificDatumWriter;
import com.linkedin.avro.fastserde.generated.avro.Fixed5;
import com.linkedin.avro.fastserde.generated.avro.SimpleTestRecord;
import com.linkedin.avroutil1.compatibility.AvroRecordUtil;

public class FastSerdeWithDataFileStreamTest {

    @DataProvider
    private Object[][] dataFileStreamDeserializationTestCases() {
        Schema readerSchema = SimpleTestRecord.SCHEMA$;
        return new Object[][]{
                new Object[]{11, new FastSpecificDatumReader<>(null, readerSchema)},
                new Object[]{12, new FastGenericDatumReader<GenericRecord>(null, readerSchema)},
        };
    }

    @Test(groups = "deserializationTest", dataProvider = "dataFileStreamDeserializationTestCases")
    <D extends IndexedRecord> void dataFileStreamShouldReadDataUsingSpecificReader(int recordsToWrite,
            DatumReader<D> datumReader) throws IOException {
        // given: records to be written to one file
        List<SimpleTestRecord> records = new ArrayList<>(recordsToWrite);
        for (byte i = 0; i < recordsToWrite; i++) {
            Fixed5 fiveBytes = new Fixed5();
            fiveBytes.bytes(new byte[]{'K', 'r', 'i', 's', i});

            SimpleTestRecord simpleTestRecord = new SimpleTestRecord();
            AvroRecordUtil.setField(simpleTestRecord, "fiveBytes", fiveBytes);
            AvroRecordUtil.setField(simpleTestRecord, "text", "text-" + i);

            records.add(simpleTestRecord);
        }

        // given: bytes array representing content of persistent file with schema and multiple records
        byte[] bytes = writeTestRecordsToFile(records);

        // when: pre-populated bytes array is consumed by DataFileStream (in tests more convenient than DataFileReader
        // because SeekableByteArrayInput is not available for older Avro versions)
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        DataFileStream<D> dataFileStream = new DataFileStream<>(inputStream, datumReader);

        // then: records read from file are the same as records sent to file
        int idx = 0;
        for (IndexedRecord recordReadFromFile : dataFileStream) {
            Assert.assertEquals(recordReadFromFile.toString(), records.get(idx++).toString());
        }
    }

    /**
     * @return bytes array representing file content
     */
    private static byte[] writeTestRecordsToFile(List<SimpleTestRecord> records) throws IOException {
        Schema schema = SimpleTestRecord.SCHEMA$;
        FastSpecificDatumWriter<SimpleTestRecord> datumWriter = new FastSpecificDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try (DataFileWriter<SimpleTestRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, outputStream);

            for (SimpleTestRecord record : records) {
                dataFileWriter.append(record);
            }

            dataFileWriter.flush();
        }

        return outputStream.toByteArray();
    }
}
