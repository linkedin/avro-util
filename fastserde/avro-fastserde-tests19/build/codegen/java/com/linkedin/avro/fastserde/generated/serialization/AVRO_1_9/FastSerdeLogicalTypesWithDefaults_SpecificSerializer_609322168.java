
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_9;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults;
import com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastSerdeLogicalTypesWithDefaults_SpecificSerializer_609322168
    implements FastSerializer<FastSerdeLogicalTypesWithDefaults>
{

    private final org.apache.avro.data.TimeConversions.DateConversion conversion_date = new org.apache.avro.data.TimeConversions.DateConversion();
    private final org.apache.avro.data.TimeConversions.TimeMillisConversion conversion_time_millis = new org.apache.avro.data.TimeConversions.TimeMillisConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMillisConversion conversion_timestamp_millis = new org.apache.avro.data.TimeConversions.TimestampMillisConversion();
    private final Schema logicalTypeSchema__419105534 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"time-millis\"}");
    private final Schema logicalTypeSchema__59052268 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
    private final Schema logicalTypeSchema_1074306973 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
    private final Schema logicalTypeSchema__252110737 = Schema.parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}");
    private final Conversions.DecimalConversion conversion_decimal = new Conversions.DecimalConversion();
    private final Schema logicalTypeSchema__1252781617 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}");
    private final org.apache.avro.data.TimeConversions.TimestampMicrosConversion conversion_timestamp_micros = new org.apache.avro.data.TimeConversions.TimestampMicrosConversion();
    private final Schema logicalTypeSchema__1515894331 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"time-micros\"}");
    private final org.apache.avro.data.TimeConversions.TimeMicrosConversion conversion_time_micros = new org.apache.avro.data.TimeConversions.TimeMicrosConversion();

    public void serialize(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        serializeFastSerdeLogicalTypesWithDefaults0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeLogicalTypesWithDefaults0(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object unionOfArrayAndMap0 = ((Object) data.get(0));
        if (unionOfArrayAndMap0 instanceof List) {
            (encoder).writeIndex(0);
            (encoder).writeArrayStart();
            if ((((List<LocalTime> ) unionOfArrayAndMap0) == null)||((List<LocalTime> ) unionOfArrayAndMap0).isEmpty()) {
                (encoder).setItemCount(0);
            } else {
                (encoder).setItemCount(((List<LocalTime> ) unionOfArrayAndMap0).size());
                Object array0 = ((List<LocalTime> ) unionOfArrayAndMap0);
                if (array0 instanceof PrimitiveIntList) {
                    PrimitiveIntList primitiveList0 = ((PrimitiveIntList) array0);
                    for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                        (encoder).startItem();
                        Object convertedValue0 = primitiveList0 .getPrimitive(counter0);
                        convertedValue0 = Conversions.convertToRawType(convertedValue0, this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis);
                        (encoder).writeInt(((Integer) convertedValue0));
                    }
                } else {
                    for (int counter1 = 0; (counter1 <((List<LocalTime> ) unionOfArrayAndMap0).size()); counter1 ++) {
                        (encoder).startItem();
                        Object convertedValue1 = ((List<LocalTime> ) unionOfArrayAndMap0).get(counter1);
                        convertedValue1 = Conversions.convertToRawType(convertedValue1, this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis);
                        (encoder).writeInt(((Integer) convertedValue1));
                    }
                }
            }
            (encoder).writeArrayEnd();
        } else {
            if (unionOfArrayAndMap0 instanceof Map) {
                (encoder).writeIndex(1);
                (encoder).writeMapStart();
                if ((((Map<CharSequence, LocalDate> ) unionOfArrayAndMap0) == null)||((Map<CharSequence, LocalDate> ) unionOfArrayAndMap0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((Map<CharSequence, LocalDate> ) unionOfArrayAndMap0).size());
                    for (CharSequence key0 : ((Map<CharSequence, LocalDate> )((Map<CharSequence, LocalDate> ) unionOfArrayAndMap0)).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key0);
                        Object convertedValue2 = ((Map<CharSequence, LocalDate> ) unionOfArrayAndMap0).get(key0);
                        convertedValue2 = Conversions.convertToRawType(convertedValue2, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                        (encoder).writeInt(((Integer) convertedValue2));
                    }
                }
                (encoder).writeMapEnd();
            }
        }
        serialize_FastSerdeLogicalTypesWithDefaults0(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults1(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults2(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults3(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults4(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults5(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults0(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Map<CharSequence, Object> mapOfUnionsOfDateAndTimestampMillis0 = ((Map<CharSequence, Object> ) data.get(1));
        (encoder).writeMapStart();
        if ((mapOfUnionsOfDateAndTimestampMillis0 == null)||mapOfUnionsOfDateAndTimestampMillis0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(mapOfUnionsOfDateAndTimestampMillis0 .size());
            for (CharSequence key1 : ((Map<CharSequence, Object> ) mapOfUnionsOfDateAndTimestampMillis0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key1);
                Object union0 = null;
                union0 = ((Map<CharSequence, Object> ) mapOfUnionsOfDateAndTimestampMillis0).get(key1);
                if (union0 instanceof LocalDate) {
                    (encoder).writeIndex(0);
                    Object convertedValue3 = union0;
                    convertedValue3 = Conversions.convertToRawType(convertedValue3, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                    (encoder).writeInt(((Integer) convertedValue3));
                } else {
                    if (union0 instanceof Instant) {
                        (encoder).writeIndex(1);
                        Object convertedValue4 = union0;
                        convertedValue4 = Conversions.convertToRawType(convertedValue4, this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis);
                        (encoder).writeLong(((Long) convertedValue4));
                    }
                }
            }
        }
        (encoder).writeMapEnd();
        Map<CharSequence, Instant> timestampMillisMap0 = ((Map<CharSequence, Instant> ) data.get(2));
        (encoder).writeMapStart();
        if ((timestampMillisMap0 == null)||timestampMillisMap0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(timestampMillisMap0 .size());
            for (CharSequence key2 : ((Map<CharSequence, Instant> ) timestampMillisMap0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key2);
                Object convertedValue5 = timestampMillisMap0 .get(key2);
                convertedValue5 = Conversions.convertToRawType(convertedValue5, this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis);
                (encoder).writeLong(((Long) convertedValue5));
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults1(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        List<LocalDate> nullableArrayOfDates0 = ((List<LocalDate> ) data.get(3));
        if (nullableArrayOfDates0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (nullableArrayOfDates0 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<LocalDate> ) nullableArrayOfDates0) == null)||((List<LocalDate> ) nullableArrayOfDates0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<LocalDate> ) nullableArrayOfDates0).size());
                    Object array1 = ((List<LocalDate> ) nullableArrayOfDates0);
                    if (array1 instanceof PrimitiveIntList) {
                        PrimitiveIntList primitiveList1 = ((PrimitiveIntList) array1);
                        for (int counter2 = 0; (counter2 <primitiveList1 .size()); counter2 ++) {
                            (encoder).startItem();
                            Object convertedValue6 = primitiveList1 .getPrimitive(counter2);
                            convertedValue6 = Conversions.convertToRawType(convertedValue6, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                            (encoder).writeInt(((Integer) convertedValue6));
                        }
                    } else {
                        for (int counter3 = 0; (counter3 <((List<LocalDate> ) nullableArrayOfDates0).size()); counter3 ++) {
                            (encoder).startItem();
                            Object convertedValue7 = ((List<LocalDate> ) nullableArrayOfDates0).get(counter3);
                            convertedValue7 = Conversions.convertToRawType(convertedValue7, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                            (encoder).writeInt(((Integer) convertedValue7));
                        }
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        List<LocalDate> arrayOfDates0 = ((List<LocalDate> ) data.get(4));
        (encoder).writeArrayStart();
        if ((arrayOfDates0 == null)||arrayOfDates0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(arrayOfDates0 .size());
            Object array2 = arrayOfDates0;
            if (array2 instanceof PrimitiveIntList) {
                PrimitiveIntList primitiveList2 = ((PrimitiveIntList) array2);
                for (int counter4 = 0; (counter4 <primitiveList2 .size()); counter4 ++) {
                    (encoder).startItem();
                    Object convertedValue8 = primitiveList2 .getPrimitive(counter4);
                    convertedValue8 = Conversions.convertToRawType(convertedValue8, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                    (encoder).writeInt(((Integer) convertedValue8));
                }
            } else {
                for (int counter5 = 0; (counter5 <arrayOfDates0 .size()); counter5 ++) {
                    (encoder).startItem();
                    Object convertedValue9 = arrayOfDates0 .get(counter5);
                    convertedValue9 = Conversions.convertToRawType(convertedValue9, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                    (encoder).writeInt(((Integer) convertedValue9));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults2(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object unionOfDecimalOrDate0 = ((Object) data.get(5));
        if (unionOfDecimalOrDate0 instanceof BigDecimal) {
            (encoder).writeIndex(0);
            Object convertedValue10 = unionOfDecimalOrDate0;
            convertedValue10 = Conversions.convertToRawType(convertedValue10, this.logicalTypeSchema__252110737, this.logicalTypeSchema__252110737 .getLogicalType(), this.conversion_decimal);
            (encoder).writeBytes(((ByteBuffer) convertedValue10));
        } else {
            if (unionOfDecimalOrDate0 instanceof LocalDate) {
                (encoder).writeIndex(1);
                Object convertedValue11 = unionOfDecimalOrDate0;
                convertedValue11 = Conversions.convertToRawType(convertedValue11, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                (encoder).writeInt(((Integer) convertedValue11));
            }
        }
        if (((CharSequence) data.get(6)) instanceof Utf8) {
            (encoder).writeString(((Utf8)((CharSequence) data.get(6))));
        } else {
            (encoder).writeString(((CharSequence) data.get(6)).toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults3(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object convertedValue12 = data.get(7);
        convertedValue12 = Conversions.convertToRawType(convertedValue12, this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis);
        (encoder).writeLong(((Long) convertedValue12));
        Object convertedValue13 = data.get(8);
        convertedValue13 = Conversions.convertToRawType(convertedValue13, this.logicalTypeSchema__1252781617, this.logicalTypeSchema__1252781617 .getLogicalType(), this.conversion_timestamp_micros);
        (encoder).writeLong(((Long) convertedValue13));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults4(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object convertedValue14 = data.get(9);
        convertedValue14 = Conversions.convertToRawType(convertedValue14, this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis);
        (encoder).writeInt(((Integer) convertedValue14));
        Object convertedValue15 = data.get(10);
        convertedValue15 = Conversions.convertToRawType(convertedValue15, this.logicalTypeSchema__1515894331, this.logicalTypeSchema__1515894331 .getLogicalType(), this.conversion_time_micros);
        (encoder).writeLong(((Long) convertedValue15));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults5(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object convertedValue16 = data.get(11);
        convertedValue16 = Conversions.convertToRawType(convertedValue16, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
        (encoder).writeInt(((Integer) convertedValue16));
        LocalTimestampRecordWithDefaults nestedLocalTimestampMillis0 = ((LocalTimestampRecordWithDefaults) data.get(12));
        serializeLocalTimestampRecordWithDefaults0(nestedLocalTimestampMillis0, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeLocalTimestampRecordWithDefaults0(LocalTimestampRecordWithDefaults data, Encoder encoder)
        throws IOException
    {
        (encoder).writeLong(((Long) data.get(0)));
        serialize_LocalTimestampRecordWithDefaults0(data, (encoder));
        serialize_LocalTimestampRecordWithDefaults1(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_LocalTimestampRecordWithDefaults0(LocalTimestampRecordWithDefaults data, Encoder encoder)
        throws IOException
    {
        Long nullableNestedTimestamp0 = ((Long) data.get(1));
        if (nullableNestedTimestamp0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeLong(((Long) nullableNestedTimestamp0));
        }
        Object nullableUnionOfDateAndLocalTimestamp0 = ((Object) data.get(2));
        if (nullableUnionOfDateAndLocalTimestamp0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (nullableUnionOfDateAndLocalTimestamp0 instanceof LocalDate) {
                (encoder).writeIndex(1);
                Object convertedValue17 = nullableUnionOfDateAndLocalTimestamp0;
                convertedValue17 = Conversions.convertToRawType(convertedValue17, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                (encoder).writeInt(((Integer) convertedValue17));
            } else {
                if (nullableUnionOfDateAndLocalTimestamp0 instanceof Long) {
                    (encoder).writeIndex(2);
                    (encoder).writeLong(((Long) nullableUnionOfDateAndLocalTimestamp0));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_LocalTimestampRecordWithDefaults1(LocalTimestampRecordWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object unionOfDateAndLocalTimestamp0 = ((Object) data.get(3));
        if (unionOfDateAndLocalTimestamp0 instanceof LocalDate) {
            (encoder).writeIndex(0);
            Object convertedValue18 = unionOfDateAndLocalTimestamp0;
            convertedValue18 = Conversions.convertToRawType(convertedValue18, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
            (encoder).writeInt(((Integer) convertedValue18));
        } else {
            if (unionOfDateAndLocalTimestamp0 instanceof Long) {
                (encoder).writeIndex(1);
                (encoder).writeLong(((Long) unionOfDateAndLocalTimestamp0));
            }
        }
    }

}
