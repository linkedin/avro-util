
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class FastSerdeLogicalTypesDefined_SpecificDeserializer_229156053_229156053
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined>
{

    private final Schema readerSchema;

    public FastSerdeLogicalTypesDefined_SpecificDeserializer_229156053_229156053(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined deserialize(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesDefined0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined deserializeFastSerdeLogicalTypesDefined0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined FastSerdeLogicalTypesDefined;
        if ((reuse)!= null) {
            FastSerdeLogicalTypesDefined = ((com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined)(reuse));
        } else {
            FastSerdeLogicalTypesDefined = new com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined();
        }
        FastSerdeLogicalTypesDefined.put(0, (decoder.readInt()));
        populate_FastSerdeLogicalTypesDefined0((FastSerdeLogicalTypesDefined), (decoder));
        return FastSerdeLogicalTypesDefined;
    }

    private void populate_FastSerdeLogicalTypesDefined0(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined FastSerdeLogicalTypesDefined, Decoder decoder)
        throws IOException
    {
        FastSerdeLogicalTypesDefined.put(1, (decoder.readInt()));
        List<Object> arrayOfUnionOfDateAndTimestampMillis0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastSerdeLogicalTypesDefined.get(2);
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
        FastSerdeLogicalTypesDefined.put(2, arrayOfUnionOfDateAndTimestampMillis0);
    }

}
