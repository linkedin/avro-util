
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class FastSerdeLogicalTypesUndefined_SpecificDeserializer_1982763418_1982763418
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined>
{

    private final Schema readerSchema;
    private final org.apache.avro.data.TimeConversions.TimeMicrosConversion conversion_time_micros = new org.apache.avro.data.TimeConversions.TimeMicrosConversion();
    private final org.apache.avro.data.TimeConversions.TimestampMicrosConversion conversion_timestamp_micros = new org.apache.avro.data.TimeConversions.TimestampMicrosConversion();
    private final org.apache.avro.Conversions.DecimalConversion conversion_decimal = new org.apache.avro.Conversions.DecimalConversion();

    public FastSerdeLogicalTypesUndefined_SpecificDeserializer_1982763418_1982763418(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined deserialize(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesUndefined0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined deserializeFastSerdeLogicalTypesUndefined0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined FastSerdeLogicalTypesUndefined;
        if ((reuse)!= null) {
            FastSerdeLogicalTypesUndefined = ((com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined)(reuse));
        } else {
            FastSerdeLogicalTypesUndefined = new com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined();
        }
        FastSerdeLogicalTypesUndefined.put(0, (decoder.readInt()));
        populate_FastSerdeLogicalTypesUndefined0((FastSerdeLogicalTypesUndefined), (decoder));
        return FastSerdeLogicalTypesUndefined;
    }

    private void populate_FastSerdeLogicalTypesUndefined0(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined FastSerdeLogicalTypesUndefined, Decoder decoder)
        throws IOException
    {
        FastSerdeLogicalTypesUndefined.put(1, (decoder.readInt()));
        List<Object> arrayOfUnionOfDateAndTimestampMillis0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastSerdeLogicalTypesUndefined.get(2);
        if (oldArray0 instanceof List) {
            arrayOfUnionOfDateAndTimestampMillis0 = ((List) oldArray0);
            arrayOfUnionOfDateAndTimestampMillis0 .clear();
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
                    arrayOfUnionOfDateAndTimestampMillis0 .add((decoder.readInt()));
                } else {
                    if (unionIndex0 == 1) {
                        arrayOfUnionOfDateAndTimestampMillis0 .add((decoder.readLong()));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'arrayOfUnionOfDateAndTimestampMillisElem': "+ unionIndex0));
                    }
                }
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastSerdeLogicalTypesUndefined.put(2, arrayOfUnionOfDateAndTimestampMillis0);
    }

}
