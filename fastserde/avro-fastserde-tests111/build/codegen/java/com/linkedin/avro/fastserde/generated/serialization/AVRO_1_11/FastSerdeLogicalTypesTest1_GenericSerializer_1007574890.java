
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastSerdeLogicalTypesTest1_GenericSerializer_1007574890
    implements FastSerializer<IndexedRecord>
{

    private final org.apache.avro.data.TimeConversions.DateConversion conversion_date = new org.apache.avro.data.TimeConversions.DateConversion();
    private final org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion conversion_local_timestamp_millis = new org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion();
    private final org.apache.avro.data.TimeConversions.TimeMicrosConversion conversion_time_micros = new org.apache.avro.data.TimeConversions.TimeMicrosConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMicrosConversion conversion_timestamp_micros = new org.apache.avro.data.TimeConversions.TimestampMicrosConversion();
    private final org.apache.avro.data.TimeConversions.TimeMillisConversion conversion_time_millis = new org.apache.avro.data.TimeConversions.TimeMillisConversion();
    private final Conversions.DecimalConversion conversion_decimal = new Conversions.DecimalConversion();
    private final Conversions.UUIDConversion conversion_uuid = new Conversions.UUIDConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMillisConversion conversion_timestamp_millis = new org.apache.avro.data.TimeConversions.TimestampMillisConversion();
    private final Schema logicalTypeSchema__419105534 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"time-millis\"}");
    private final Schema logicalTypeSchema__59052268 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
    private final Schema logicalTypeSchema_1074306973 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");
    private final Schema logicalTypeSchema_120893213 = Schema.parse("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}");
    private final Schema logicalTypeSchema__1245572876 = Schema.parse("{\"type\":\"string\",\"logicalType\":\"uuid\"}");
    private final Schema logicalTypeSchema__1252781617 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}");
    private final Schema logicalTypeSchema__1515894331 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"time-micros\"}");
    private final Schema logicalTypeSchema__250645780 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}");

    public void serialize(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        serializeFastSerdeLogicalTypesTest10(data, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeLogicalTypesTest10(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
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
                (customization).getCheckMapTypeFunction().apply(((Map<CharSequence, LocalDate> ) unionOfArrayAndMap0));
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
        serialize_FastSerdeLogicalTypesTest10(data, (encoder), (customization));
        serialize_FastSerdeLogicalTypesTest11(data, (encoder), (customization));
        serialize_FastSerdeLogicalTypesTest12(data, (encoder), (customization));
        serialize_FastSerdeLogicalTypesTest13(data, (encoder), (customization));
        serialize_FastSerdeLogicalTypesTest14(data, (encoder), (customization));
        serialize_FastSerdeLogicalTypesTest15(data, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesTest10(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        Map<CharSequence, Object> mapOfUnionsOfDateAndTimestampMillis0 = ((Map<CharSequence, Object> ) data.get(1));
        (customization).getCheckMapTypeFunction().apply(mapOfUnionsOfDateAndTimestampMillis0);
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
        (customization).getCheckMapTypeFunction().apply(timestampMillisMap0);
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
    private void serialize_FastSerdeLogicalTypesTest11(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
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
    private void serialize_FastSerdeLogicalTypesTest12(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        Object unionOfDecimalOrDate0 = ((Object) data.get(5));
        if (unionOfDecimalOrDate0 instanceof BigDecimal) {
            (encoder).writeIndex(0);
            Object convertedValue10 = unionOfDecimalOrDate0;
            convertedValue10 = Conversions.convertToRawType(convertedValue10, this.logicalTypeSchema_120893213, this.logicalTypeSchema_120893213 .getLogicalType(), this.conversion_decimal);
            (encoder).writeBytes(((ByteBuffer) convertedValue10));
        } else {
            if (unionOfDecimalOrDate0 instanceof LocalDate) {
                (encoder).writeIndex(1);
                Object convertedValue11 = unionOfDecimalOrDate0;
                convertedValue11 = Conversions.convertToRawType(convertedValue11, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                (encoder).writeInt(((Integer) convertedValue11));
            }
        }
        Object convertedValue12 = data.get(6);
        convertedValue12 = Conversions.convertToRawType(convertedValue12, this.logicalTypeSchema__1245572876, this.logicalTypeSchema__1245572876 .getLogicalType(), this.conversion_uuid);
        if (((CharSequence) convertedValue12) instanceof Utf8) {
            (encoder).writeString(((Utf8)((CharSequence) convertedValue12)));
        } else {
            (encoder).writeString(((CharSequence) convertedValue12).toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesTest13(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        Object convertedValue13 = data.get(7);
        convertedValue13 = Conversions.convertToRawType(convertedValue13, this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis);
        (encoder).writeLong(((Long) convertedValue13));
        Object convertedValue14 = data.get(8);
        convertedValue14 = Conversions.convertToRawType(convertedValue14, this.logicalTypeSchema__1252781617, this.logicalTypeSchema__1252781617 .getLogicalType(), this.conversion_timestamp_micros);
        (encoder).writeLong(((Long) convertedValue14));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesTest14(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        Object convertedValue15 = data.get(9);
        convertedValue15 = Conversions.convertToRawType(convertedValue15, this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis);
        (encoder).writeInt(((Integer) convertedValue15));
        Object convertedValue16 = data.get(10);
        convertedValue16 = Conversions.convertToRawType(convertedValue16, this.logicalTypeSchema__1515894331, this.logicalTypeSchema__1515894331 .getLogicalType(), this.conversion_time_micros);
        (encoder).writeLong(((Long) convertedValue16));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesTest15(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        Object convertedValue17 = data.get(11);
        convertedValue17 = Conversions.convertToRawType(convertedValue17, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
        (encoder).writeInt(((Integer) convertedValue17));
        IndexedRecord nestedLocalTimestampMillis0 = ((IndexedRecord) data.get(12));
        serializeLocalTimestampRecord0(nestedLocalTimestampMillis0, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    public void serializeLocalTimestampRecord0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        Object convertedValue18 = data.get(0);
        convertedValue18 = Conversions.convertToRawType(convertedValue18, this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis);
        (encoder).writeLong(((Long) convertedValue18));
        serialize_LocalTimestampRecord0(data, (encoder), (customization));
        serialize_LocalTimestampRecord1(data, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    private void serialize_LocalTimestampRecord0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        LocalDateTime nullableNestedTimestamp0 = ((LocalDateTime) data.get(1));
        if (nullableNestedTimestamp0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            Object convertedValue19 = nullableNestedTimestamp0;
            convertedValue19 = Conversions.convertToRawType(convertedValue19, this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis);
            (encoder).writeLong(((Long) convertedValue19));
        }
        Object nullableUnionOfDateAndLocalTimestamp0 = ((Object) data.get(2));
        if (nullableUnionOfDateAndLocalTimestamp0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (nullableUnionOfDateAndLocalTimestamp0 instanceof LocalDate) {
                (encoder).writeIndex(1);
                Object convertedValue20 = nullableUnionOfDateAndLocalTimestamp0;
                convertedValue20 = Conversions.convertToRawType(convertedValue20, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                (encoder).writeInt(((Integer) convertedValue20));
            } else {
                if (nullableUnionOfDateAndLocalTimestamp0 instanceof LocalDateTime) {
                    (encoder).writeIndex(2);
                    Object convertedValue21 = nullableUnionOfDateAndLocalTimestamp0;
                    convertedValue21 = Conversions.convertToRawType(convertedValue21, this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis);
                    (encoder).writeLong(((Long) convertedValue21));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_LocalTimestampRecord1(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        Object unionOfDateAndLocalTimestamp0 = ((Object) data.get(3));
        if (unionOfDateAndLocalTimestamp0 instanceof LocalDate) {
            (encoder).writeIndex(0);
            Object convertedValue22 = unionOfDateAndLocalTimestamp0;
            convertedValue22 = Conversions.convertToRawType(convertedValue22, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
            (encoder).writeInt(((Integer) convertedValue22));
        } else {
            if (unionOfDateAndLocalTimestamp0 instanceof LocalDateTime) {
                (encoder).writeIndex(1);
                Object convertedValue23 = unionOfDateAndLocalTimestamp0;
                convertedValue23 = Conversions.convertToRawType(convertedValue23, this.logicalTypeSchema__250645780, this.logicalTypeSchema__250645780 .getLogicalType(), this.conversion_local_timestamp_millis);
                (encoder).writeLong(((Long) convertedValue23));
            }
        }
    }

}
