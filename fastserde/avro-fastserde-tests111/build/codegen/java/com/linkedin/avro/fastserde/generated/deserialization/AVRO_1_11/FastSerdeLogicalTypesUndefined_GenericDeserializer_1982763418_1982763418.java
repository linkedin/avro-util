
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastSerdeLogicalTypesUndefined_GenericDeserializer_1982763418_1982763418
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema arrayOfUnionOfDateAndTimestampMillis0;
    private final Schema arrayOfUnionOfDateAndTimestampMillisArrayElemSchema0;

    public FastSerdeLogicalTypesUndefined_GenericDeserializer_1982763418_1982763418(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayOfUnionOfDateAndTimestampMillis0 = readerSchema.getField("arrayOfUnionOfDateAndTimestampMillis").schema();
        this.arrayOfUnionOfDateAndTimestampMillisArrayElemSchema0 = arrayOfUnionOfDateAndTimestampMillis0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesUndefined0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastSerdeLogicalTypesUndefined0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastSerdeLogicalTypesUndefined;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastSerdeLogicalTypesUndefined = ((IndexedRecord)(reuse));
        } else {
            FastSerdeLogicalTypesUndefined = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastSerdeLogicalTypesUndefined.put(0, (decoder.readInt()));
        populate_FastSerdeLogicalTypesUndefined0((FastSerdeLogicalTypesUndefined), (decoder));
        return FastSerdeLogicalTypesUndefined;
    }

    private void populate_FastSerdeLogicalTypesUndefined0(IndexedRecord FastSerdeLogicalTypesUndefined, Decoder decoder)
        throws IOException
    {
        FastSerdeLogicalTypesUndefined.put(1, (decoder.readInt()));
        List<Object> arrayOfUnionOfDateAndTimestampMillis1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastSerdeLogicalTypesUndefined.get(2);
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
        FastSerdeLogicalTypesUndefined.put(2, arrayOfUnionOfDateAndTimestampMillis1);
    }

}
