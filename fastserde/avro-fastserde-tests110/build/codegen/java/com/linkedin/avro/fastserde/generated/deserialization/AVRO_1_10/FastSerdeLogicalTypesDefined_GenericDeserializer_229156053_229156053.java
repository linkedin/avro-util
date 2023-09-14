
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_10;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastSerdeLogicalTypesDefined_GenericDeserializer_229156053_229156053
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema arrayOfUnionOfDateAndTimestampMillis0;
    private final Schema arrayOfUnionOfDateAndTimestampMillisArrayElemSchema0;

    public FastSerdeLogicalTypesDefined_GenericDeserializer_229156053_229156053(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayOfUnionOfDateAndTimestampMillis0 = readerSchema.getField("arrayOfUnionOfDateAndTimestampMillis").schema();
        this.arrayOfUnionOfDateAndTimestampMillisArrayElemSchema0 = arrayOfUnionOfDateAndTimestampMillis0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesDefined0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastSerdeLogicalTypesDefined0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastSerdeLogicalTypesDefined;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastSerdeLogicalTypesDefined = ((IndexedRecord)(reuse));
        } else {
            FastSerdeLogicalTypesDefined = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastSerdeLogicalTypesDefined.put(0, (decoder.readInt()));
        populate_FastSerdeLogicalTypesDefined0((FastSerdeLogicalTypesDefined), (decoder));
        return FastSerdeLogicalTypesDefined;
    }

    private void populate_FastSerdeLogicalTypesDefined0(IndexedRecord FastSerdeLogicalTypesDefined, Decoder decoder)
        throws IOException
    {
        FastSerdeLogicalTypesDefined.put(1, (decoder.readInt()));
        List<Object> arrayOfUnionOfDateAndTimestampMillis1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastSerdeLogicalTypesDefined.get(2);
        if (oldArray0 instanceof List) {
            arrayOfUnionOfDateAndTimestampMillis1 = ((List) oldArray0);
            arrayOfUnionOfDateAndTimestampMillis1 .clear();
        } else {
            arrayOfUnionOfDateAndTimestampMillis1 = new org.apache.avro.generic.GenericData.Array<Object>(((int) chunkLen0), arrayOfUnionOfDateAndTimestampMillis0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object arrayOfUnionOfDateAndTimestampMillisArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    arrayOfUnionOfDateAndTimestampMillisArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                int unionIndex0 = (decoder.readIndex());
                if (unionIndex0 == 0) {
                    arrayOfUnionOfDateAndTimestampMillis1 .add((decoder.readInt()));
                } else {
                    if (unionIndex0 == 1) {
                        arrayOfUnionOfDateAndTimestampMillis1 .add((decoder.readLong()));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'arrayOfUnionOfDateAndTimestampMillisElem': "+ unionIndex0));
                    }
                }
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastSerdeLogicalTypesDefined.put(2, arrayOfUnionOfDateAndTimestampMillis1);
    }

}
