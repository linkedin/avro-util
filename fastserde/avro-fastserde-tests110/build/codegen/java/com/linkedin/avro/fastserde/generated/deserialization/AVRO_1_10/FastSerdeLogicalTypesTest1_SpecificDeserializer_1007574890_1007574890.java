
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_10;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastSerdeLogicalTypesTest1_SpecificDeserializer_1007574890_1007574890
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1>
{

    private final Schema readerSchema;
    private final org.apache.avro.data.TimeConversions.DateConversion conversion_date = new org.apache.avro.data.TimeConversions.DateConversion();
    private final org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion conversion_local_timestamp_millis = new org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion();
    private final org.apache.avro.data.TimeConversions.TimeMicrosConversion conversion_time_micros = new org.apache.avro.data.TimeConversions.TimeMicrosConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMicrosConversion conversion_timestamp_micros = new org.apache.avro.data.TimeConversions.TimestampMicrosConversion();
    private final org.apache.avro.data.TimeConversions.TimeMillisConversion conversion_time_millis = new org.apache.avro.data.TimeConversions.TimeMillisConversion();
    private final Conversions.DecimalConversion conversion_decimal = new Conversions.DecimalConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMillisConversion conversion_timestamp_millis = new org.apache.avro.data.TimeConversions.TimestampMillisConversion();
    private final Schema logicalTypeSchema__419105534 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"time-millis\"}");
    private final Schema logicalTypeSchema__59052268 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
    private final Schema logicalTypeSchema_1074306973 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
    private final Schema logicalTypeSchema_120893213 = Schema.parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}");
    private final Schema logicalTypeSchema__1252781617 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}");
    private final Schema logicalTypeSchema__1515894331 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"time-micros\"}");
    private final Schema logicalTypeSchema__250645780 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}");

    public FastSerdeLogicalTypesTest1_SpecificDeserializer_1007574890_1007574890(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 deserialize(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesTest10((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 deserializeFastSerdeLogicalTypesTest10(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 FastSerdeLogicalTypesTest1;
        if ((reuse)!= null) {
            FastSerdeLogicalTypesTest1 = ((com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1)(reuse));
        } else {
            FastSerdeLogicalTypesTest1 = new com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            List<LocalTime> unionOfArrayAndMapOption0 = null;
            long chunkLen0 = (decoder.readArrayStart());
            Object oldArray0 = FastSerdeLogicalTypesTest1 .get(0);
            if (oldArray0 instanceof List) {
                unionOfArrayAndMapOption0 = ((List) oldArray0);
                unionOfArrayAndMapOption0 .clear();
            } else {
                unionOfArrayAndMapOption0 = new ArrayList<LocalTime>(((int) chunkLen0));
            }
            while (chunkLen0 > 0) {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    LocalTime convertedValue0 = ((LocalTime) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis));
                    unionOfArrayAndMapOption0 .add(convertedValue0);
                }
                chunkLen0 = (decoder.arrayNext());
            }
            FastSerdeLogicalTypesTest1 .put(0, unionOfArrayAndMapOption0);
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, LocalDate> unionOfArrayAndMapOption1 = null;
                long chunkLen1 = (decoder.readMapStart());
                if (chunkLen1 > 0) {
                    Map<Utf8, LocalDate> unionOfArrayAndMapOptionReuse0 = null;
                    Object oldMap0 = FastSerdeLogicalTypesTest1 .get(0);
                    if (oldMap0 instanceof Map) {
                        unionOfArrayAndMapOptionReuse0 = ((Map) oldMap0);
                    }
                    if (unionOfArrayAndMapOptionReuse0 != (null)) {
                        unionOfArrayAndMapOptionReuse0 .clear();
                        unionOfArrayAndMapOption1 = unionOfArrayAndMapOptionReuse0;
                    } else {
                        unionOfArrayAndMapOption1 = new HashMap<Utf8, LocalDate>(((int)(((chunkLen1 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            LocalDate convertedValue1 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
                            unionOfArrayAndMapOption1 .put(key0, convertedValue1);
                        }
                        chunkLen1 = (decoder.mapNext());
                    } while (chunkLen1 > 0);
                } else {
                    unionOfArrayAndMapOption1 = new HashMap<Utf8, LocalDate>(0);
                }
                FastSerdeLogicalTypesTest1 .put(0, unionOfArrayAndMapOption1);
            } else {
                throw new RuntimeException(("Illegal union index for 'unionOfArrayAndMap': "+ unionIndex0));
            }
        }
        populate_FastSerdeLogicalTypesTest10((FastSerdeLogicalTypesTest1), (decoder));
        populate_FastSerdeLogicalTypesTest11((FastSerdeLogicalTypesTest1), (decoder));
        populate_FastSerdeLogicalTypesTest12((FastSerdeLogicalTypesTest1), (decoder));
        populate_FastSerdeLogicalTypesTest13((FastSerdeLogicalTypesTest1), (decoder));
        populate_FastSerdeLogicalTypesTest14((FastSerdeLogicalTypesTest1), (decoder));
        populate_FastSerdeLogicalTypesTest15((FastSerdeLogicalTypesTest1), (decoder));
        return FastSerdeLogicalTypesTest1;
    }

    private void populate_FastSerdeLogicalTypesTest10(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 FastSerdeLogicalTypesTest1, Decoder decoder)
        throws IOException
    {
        Map<Utf8, Object> mapOfUnionsOfDateAndTimestampMillis0 = null;
        long chunkLen2 = (decoder.readMapStart());
        if (chunkLen2 > 0) {
            Map<Utf8, Object> mapOfUnionsOfDateAndTimestampMillisReuse0 = null;
            Object oldMap1 = FastSerdeLogicalTypesTest1 .get(1);
            if (oldMap1 instanceof Map) {
                mapOfUnionsOfDateAndTimestampMillisReuse0 = ((Map) oldMap1);
            }
            if (mapOfUnionsOfDateAndTimestampMillisReuse0 != (null)) {
                mapOfUnionsOfDateAndTimestampMillisReuse0 .clear();
                mapOfUnionsOfDateAndTimestampMillis0 = mapOfUnionsOfDateAndTimestampMillisReuse0;
            } else {
                mapOfUnionsOfDateAndTimestampMillis0 = new HashMap<Utf8, Object>(((int)(((chunkLen2 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                    Utf8 key1 = (decoder.readString(null));
                    int unionIndex1 = (decoder.readIndex());
                    if (unionIndex1 == 0) {
                        LocalDate convertedValue2 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
                        mapOfUnionsOfDateAndTimestampMillis0 .put(key1, convertedValue2);
                    } else {
                        if (unionIndex1 == 1) {
                            Instant convertedValue3 = ((Instant) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis));
                            mapOfUnionsOfDateAndTimestampMillis0 .put(key1, convertedValue3);
                        } else {
                            throw new RuntimeException(("Illegal union index for 'mapOfUnionsOfDateAndTimestampMillisValue': "+ unionIndex1));
                        }
                    }
                }
                chunkLen2 = (decoder.mapNext());
            } while (chunkLen2 > 0);
        } else {
            mapOfUnionsOfDateAndTimestampMillis0 = new HashMap<Utf8, Object>(0);
        }
        FastSerdeLogicalTypesTest1 .put(1, mapOfUnionsOfDateAndTimestampMillis0);
        Map<Utf8, Instant> timestampMillisMap0 = null;
        long chunkLen3 = (decoder.readMapStart());
        if (chunkLen3 > 0) {
            Map<Utf8, Instant> timestampMillisMapReuse0 = null;
            Object oldMap2 = FastSerdeLogicalTypesTest1 .get(2);
            if (oldMap2 instanceof Map) {
                timestampMillisMapReuse0 = ((Map) oldMap2);
            }
            if (timestampMillisMapReuse0 != (null)) {
                timestampMillisMapReuse0 .clear();
                timestampMillisMap0 = timestampMillisMapReuse0;
            } else {
                timestampMillisMap0 = new HashMap<Utf8, Instant>(((int)(((chunkLen3 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter3 = 0; (counter3 <chunkLen3); counter3 ++) {
                    Utf8 key2 = (decoder.readString(null));
                    Instant convertedValue4 = ((Instant) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis));
                    timestampMillisMap0 .put(key2, convertedValue4);
                }
                chunkLen3 = (decoder.mapNext());
            } while (chunkLen3 > 0);
        } else {
            timestampMillisMap0 = new HashMap<Utf8, Instant>(0);
        }
        FastSerdeLogicalTypesTest1 .put(2, timestampMillisMap0);
    }

    private void populate_FastSerdeLogicalTypesTest11(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 FastSerdeLogicalTypesTest1, Decoder decoder)
        throws IOException
    {
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            FastSerdeLogicalTypesTest1 .put(3, null);
        } else {
            if (unionIndex2 == 1) {
                List<LocalDate> nullableArrayOfDatesOption0 = null;
                long chunkLen4 = (decoder.readArrayStart());
                Object oldArray1 = FastSerdeLogicalTypesTest1 .get(3);
                if (oldArray1 instanceof List) {
                    nullableArrayOfDatesOption0 = ((List) oldArray1);
                    nullableArrayOfDatesOption0 .clear();
                } else {
                    nullableArrayOfDatesOption0 = new ArrayList<LocalDate>(((int) chunkLen4));
                }
                while (chunkLen4 > 0) {
                    for (int counter4 = 0; (counter4 <chunkLen4); counter4 ++) {
                        LocalDate convertedValue5 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
                        nullableArrayOfDatesOption0 .add(convertedValue5);
                    }
                    chunkLen4 = (decoder.arrayNext());
                }
                FastSerdeLogicalTypesTest1 .put(3, nullableArrayOfDatesOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'nullableArrayOfDates': "+ unionIndex2));
            }
        }
        List<LocalDate> arrayOfDates0 = null;
        long chunkLen5 = (decoder.readArrayStart());
        Object oldArray2 = FastSerdeLogicalTypesTest1 .get(4);
        if (oldArray2 instanceof List) {
            arrayOfDates0 = ((List) oldArray2);
            arrayOfDates0 .clear();
        } else {
            arrayOfDates0 = new ArrayList<LocalDate>(((int) chunkLen5));
        }
        while (chunkLen5 > 0) {
            for (int counter5 = 0; (counter5 <chunkLen5); counter5 ++) {
                LocalDate convertedValue6 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
                arrayOfDates0 .add(convertedValue6);
            }
            chunkLen5 = (decoder.arrayNext());
        }
        FastSerdeLogicalTypesTest1 .put(4, arrayOfDates0);
    }

    private void populate_FastSerdeLogicalTypesTest12(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 FastSerdeLogicalTypesTest1, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            ByteBuffer byteBuffer0;
            Object oldBytes0 = FastSerdeLogicalTypesTest1 .get(5);
            if (oldBytes0 instanceof ByteBuffer) {
                byteBuffer0 = (decoder).readBytes(((ByteBuffer) oldBytes0));
            } else {
                byteBuffer0 = (decoder).readBytes((null));
            }
            BigDecimal convertedValue7 = ((BigDecimal) Conversions.convertToLogicalType(byteBuffer0, this.logicalTypeSchema_120893213, this.logicalTypeSchema_120893213 .getLogicalType(), this.conversion_decimal));
            FastSerdeLogicalTypesTest1 .put(5, convertedValue7);
        } else {
            if (unionIndex3 == 1) {
                LocalDate convertedValue8 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
                FastSerdeLogicalTypesTest1 .put(5, convertedValue8);
            } else {
                throw new RuntimeException(("Illegal union index for 'unionOfDecimalOrDate': "+ unionIndex3));
            }
        }
        Utf8 charSequence0;
        Object oldString0 = FastSerdeLogicalTypesTest1 .get(6);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        FastSerdeLogicalTypesTest1 .put(6, charSequence0);
    }

    private void populate_FastSerdeLogicalTypesTest13(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 FastSerdeLogicalTypesTest1, Decoder decoder)
        throws IOException
    {
        Instant convertedValue9 = ((Instant) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis));
        FastSerdeLogicalTypesTest1 .put(7, convertedValue9);
        Instant convertedValue10 = ((Instant) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema__1252781617, this.logicalTypeSchema__1252781617 .getLogicalType(), this.conversion_timestamp_micros));
        FastSerdeLogicalTypesTest1 .put(8, convertedValue10);
    }

    private void populate_FastSerdeLogicalTypesTest14(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 FastSerdeLogicalTypesTest1, Decoder decoder)
        throws IOException
    {
        LocalTime convertedValue11 = ((LocalTime) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis));
        FastSerdeLogicalTypesTest1 .put(9, convertedValue11);
        LocalTime convertedValue12 = ((LocalTime) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema__1515894331, this.logicalTypeSchema__1515894331 .getLogicalType(), this.conversion_time_micros));
        FastSerdeLogicalTypesTest1 .put(10, convertedValue12);
    }

    private void populate_FastSerdeLogicalTypesTest15(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1 FastSerdeLogicalTypesTest1, Decoder decoder)
        throws IOException
    {
        LocalDate convertedValue13 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
        FastSerdeLogicalTypesTest1 .put(11, convertedValue13);
        FastSerdeLogicalTypesTest1 .put(12, deserializeLocalTimestampRecord0(FastSerdeLogicalTypesTest1 .get(12), (decoder)));
    }

    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord deserializeLocalTimestampRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord LocalTimestampRecord;
        if ((reuse)!= null) {
            LocalTimestampRecord = ((com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord)(reuse));
        } else {
            LocalTimestampRecord = new com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord();
        }
        LocalDateTime convertedValue14 = ((LocalDateTime) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis));
        LocalTimestampRecord.put(0, convertedValue14);
        populate_LocalTimestampRecord0((LocalTimestampRecord), (decoder));
        populate_LocalTimestampRecord1((LocalTimestampRecord), (decoder));
        return LocalTimestampRecord;
    }

    private void populate_LocalTimestampRecord0(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord LocalTimestampRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
            LocalTimestampRecord.put(1, null);
        } else {
            if (unionIndex4 == 1) {
                LocalDateTime convertedValue15 = ((LocalDateTime) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis));
                LocalTimestampRecord.put(1, convertedValue15);
            } else {
                throw new RuntimeException(("Illegal union index for 'nullableNestedTimestamp': "+ unionIndex4));
            }
        }
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
            LocalTimestampRecord.put(2, null);
        } else {
            if (unionIndex5 == 1) {
                LocalDate convertedValue16 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
                LocalTimestampRecord.put(2, convertedValue16);
            } else {
                if (unionIndex5 == 2) {
                    LocalDateTime convertedValue17 = ((LocalDateTime) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis));
                    LocalTimestampRecord.put(2, convertedValue17);
                } else {
                    throw new RuntimeException(("Illegal union index for 'nullableUnionOfDateAndLocalTimestamp': "+ unionIndex5));
                }
            }
        }
    }

    private void populate_LocalTimestampRecord1(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord LocalTimestampRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex6 = (decoder.readIndex());
        if (unionIndex6 == 0) {
            LocalDate convertedValue18 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
            LocalTimestampRecord.put(3, convertedValue18);
        } else {
            if (unionIndex6 == 1) {
                LocalDateTime convertedValue19 = ((LocalDateTime) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis));
                LocalTimestampRecord.put(3, convertedValue19);
            } else {
                throw new RuntimeException(("Illegal union index for 'unionOfDateAndLocalTimestamp': "+ unionIndex6));
            }
        }
    }

}
