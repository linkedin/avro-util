
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class FastSerdeLogicalTypesDefined_SpecificDeserializer_229156053_229156053
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined>
{

    private final Schema readerSchema;
    private final org.apache.avro.data.TimeConversions.DateConversion conversion_date = new org.apache.avro.data.TimeConversions.DateConversion();
    private final org.apache.avro.data.TimeConversions.TimeMillisConversion conversion_time_millis = new org.apache.avro.data.TimeConversions.TimeMillisConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMillisConversion conversion_timestamp_millis = new org.apache.avro.data.TimeConversions.TimestampMillisConversion();
    private final Schema logicalTypeSchema__419105534 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"time-millis\"}");
    private final Schema logicalTypeSchema__59052268 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
    private final Schema logicalTypeSchema_1074306973 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");

    public FastSerdeLogicalTypesDefined_SpecificDeserializer_229156053_229156053(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined deserialize(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesDefined0((reuse), (decoder), (customization));
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined deserializeFastSerdeLogicalTypesDefined0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined FastSerdeLogicalTypesDefined;
        if ((reuse)!= null) {
            FastSerdeLogicalTypesDefined = ((com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined)(reuse));
        } else {
            FastSerdeLogicalTypesDefined = new com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined();
        }
        LocalTime convertedValue0 = ((LocalTime) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis));
        FastSerdeLogicalTypesDefined.put(0, convertedValue0);
        populate_FastSerdeLogicalTypesDefined0((FastSerdeLogicalTypesDefined), (customization), (decoder));
        return FastSerdeLogicalTypesDefined;
    }

    private void populate_FastSerdeLogicalTypesDefined0(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined FastSerdeLogicalTypesDefined, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        LocalDate convertedValue1 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
        FastSerdeLogicalTypesDefined.put(1, convertedValue1);
        List<Object> arrayOfUnionOfDateAndTimestampMillis0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastSerdeLogicalTypesDefined.get(2);
        if (oldArray0 instanceof List) {
            arrayOfUnionOfDateAndTimestampMillis0 = ((List) oldArray0);
            if (arrayOfUnionOfDateAndTimestampMillis0 instanceof GenericArray) {
                ((GenericArray) arrayOfUnionOfDateAndTimestampMillis0).reset();
            } else {
                arrayOfUnionOfDateAndTimestampMillis0 .clear();
            }
        } else {
            arrayOfUnionOfDateAndTimestampMillis0 = new ArrayList<Object>(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object arrayOfUnionOfDateAndTimestampMillisArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    arrayOfUnionOfDateAndTimestampMillisArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                int unionIndex0 = (decoder.readIndex());
                if (unionIndex0 == 0) {
                    LocalDate convertedValue2 = ((LocalDate) Conversions.convertToLogicalType((decoder.readInt()), this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date));
                    arrayOfUnionOfDateAndTimestampMillis0 .add(convertedValue2);
                } else {
                    if (unionIndex0 == 1) {
                        Instant convertedValue3 = ((Instant) Conversions.convertToLogicalType((decoder.readLong()), this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis));
                        arrayOfUnionOfDateAndTimestampMillis0 .add(convertedValue3);
                    } else {
                        throw new RuntimeException(("Illegal union index for 'arrayOfUnionOfDateAndTimestampMillisElem': "+ unionIndex0));
                    }
                }
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastSerdeLogicalTypesDefined.put(2, arrayOfUnionOfDateAndTimestampMillis0);
    }

}
