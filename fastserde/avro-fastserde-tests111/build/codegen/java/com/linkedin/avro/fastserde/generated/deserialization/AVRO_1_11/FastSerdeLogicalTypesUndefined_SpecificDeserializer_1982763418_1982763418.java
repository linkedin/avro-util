
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class FastSerdeLogicalTypesUndefined_SpecificDeserializer_1982763418_1982763418
    implements FastDeserializer<FastSerdeLogicalTypesUndefined>
{

    private final Schema readerSchema;

    public FastSerdeLogicalTypesUndefined_SpecificDeserializer_1982763418_1982763418(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public FastSerdeLogicalTypesUndefined deserialize(FastSerdeLogicalTypesUndefined reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesUndefined0((reuse), (decoder), (customization));
    }

    public FastSerdeLogicalTypesUndefined deserializeFastSerdeLogicalTypesUndefined0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        FastSerdeLogicalTypesUndefined fastSerdeLogicalTypesUndefined0;
        if ((reuse)!= null) {
            fastSerdeLogicalTypesUndefined0 = ((FastSerdeLogicalTypesUndefined)(reuse));
        } else {
            fastSerdeLogicalTypesUndefined0 = new FastSerdeLogicalTypesUndefined();
        }
        fastSerdeLogicalTypesUndefined0 .put(0, (decoder.readInt()));
        populate_FastSerdeLogicalTypesUndefined0((fastSerdeLogicalTypesUndefined0), (customization), (decoder));
        return fastSerdeLogicalTypesUndefined0;
    }

    private void populate_FastSerdeLogicalTypesUndefined0(FastSerdeLogicalTypesUndefined fastSerdeLogicalTypesUndefined0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        fastSerdeLogicalTypesUndefined0 .put(1, (decoder.readInt()));
        List<Object> arrayOfUnionOfDateAndTimestampMillis0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = fastSerdeLogicalTypesUndefined0 .get(2);
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
        fastSerdeLogicalTypesUndefined0 .put(2, arrayOfUnionOfDateAndTimestampMillis0);
    }

}
