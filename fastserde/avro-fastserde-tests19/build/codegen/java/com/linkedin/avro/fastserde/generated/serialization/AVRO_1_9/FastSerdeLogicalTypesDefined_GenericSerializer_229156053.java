
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_9;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastSerdeLogicalTypesDefined_GenericSerializer_229156053
    implements FastSerializer<IndexedRecord>
{

    private final org.apache.avro.data.TimeConversions.DateConversion conversion_date = new org.apache.avro.data.TimeConversions.DateConversion();
    private final org.apache.avro.data.TimeConversions.TimeMicrosConversion conversion_time_micros = new org.apache.avro.data.TimeConversions.TimeMicrosConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMicrosConversion conversion_timestamp_micros = new org.apache.avro.data.TimeConversions.TimestampMicrosConversion();
    private final org.apache.avro.data.TimeConversions.TimeMillisConversion conversion_time_millis = new org.apache.avro.data.TimeConversions.TimeMillisConversion();
    private final Conversions.DecimalConversion conversion_decimal = new Conversions.DecimalConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMillisConversion conversion_timestamp_millis = new org.apache.avro.data.TimeConversions.TimestampMillisConversion();
    private final Schema logicalTypeSchema__419105534 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"time-millis\"}");
    private final Schema logicalTypeSchema__59052268 = Schema.parse("{\"type\":\"int\",\"logicalType\":\"date\"}");
    private final Schema logicalTypeSchema_1074306973 = Schema.parse("{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}");

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastSerdeLogicalTypesDefined0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeLogicalTypesDefined0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Object convertedValue0 = data.get(0);
        convertedValue0 = Conversions.convertToRawType(convertedValue0, this.logicalTypeSchema__419105534, this.logicalTypeSchema__419105534 .getLogicalType(), this.conversion_time_millis);
        (encoder).writeInt(((Integer) convertedValue0));
        serialize_FastSerdeLogicalTypesDefined0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesDefined0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Object convertedValue1 = data.get(1);
        convertedValue1 = Conversions.convertToRawType(convertedValue1, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
        (encoder).writeInt(((Integer) convertedValue1));
        List<Object> arrayOfUnionOfDateAndTimestampMillis0 = ((List<Object> ) data.get(2));
        (encoder).writeArrayStart();
        if ((arrayOfUnionOfDateAndTimestampMillis0 == null)||arrayOfUnionOfDateAndTimestampMillis0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(arrayOfUnionOfDateAndTimestampMillis0 .size());
            for (int counter0 = 0; (counter0 <arrayOfUnionOfDateAndTimestampMillis0 .size()); counter0 ++) {
                (encoder).startItem();
                Object union0 = null;
                union0 = ((List<Object> ) arrayOfUnionOfDateAndTimestampMillis0).get(counter0);
                if (union0 instanceof LocalDate) {
                    (encoder).writeIndex(0);
                    Object convertedValue2 = union0;
                    convertedValue2 = Conversions.convertToRawType(convertedValue2, this.logicalTypeSchema__59052268, this.logicalTypeSchema__59052268 .getLogicalType(), this.conversion_date);
                    (encoder).writeInt(((Integer) convertedValue2));
                } else {
                    if (union0 instanceof Instant) {
                        (encoder).writeIndex(1);
                        Object convertedValue3 = union0;
                        convertedValue3 = Conversions.convertToRawType(convertedValue3, this.logicalTypeSchema_1074306973, this.logicalTypeSchema_1074306973 .getLogicalType(), this.conversion_timestamp_millis);
                        (encoder).writeLong(((Long) convertedValue3));
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
