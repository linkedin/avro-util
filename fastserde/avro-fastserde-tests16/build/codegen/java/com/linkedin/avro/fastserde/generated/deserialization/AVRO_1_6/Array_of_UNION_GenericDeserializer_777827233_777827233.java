
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Array_of_UNION_GenericDeserializer_777827233_777827233
    implements FastDeserializer<List<IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema arrayArrayElemSchema0;
    private final Schema arrayElemOptionSchema0;
    private final Schema field0;

    public Array_of_UNION_GenericDeserializer_777827233_777827233(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayArrayElemSchema0 = readerSchema.getElementType();
        this.arrayElemOptionSchema0 = arrayArrayElemSchema0 .getTypes().get(1);
        this.field0 = arrayElemOptionSchema0 .getField("field").schema();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        List<IndexedRecord> array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if ((reuse) instanceof List) {
            array0 = ((List)(reuse));
            array0 .clear();
        } else {
            array0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), readerSchema);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object arrayArrayElementReuseVar0 = null;
                if ((reuse) instanceof GenericArray) {
                    arrayArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                }
                int unionIndex0 = (decoder.readIndex());
                if (unionIndex0 == 0) {
                    decoder.readNull();
                    array0 .add(null);
                } else {
                    if (unionIndex0 == 1) {
                        array0 .add(deserializerecord0(arrayArrayElementReuseVar0, (decoder)));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'arrayElem': "+ unionIndex0));
                    }
                }
            }
            chunkLen0 = (decoder.arrayNext());
        }
        return array0;
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == arrayElemOptionSchema0)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(arrayElemOptionSchema0);
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            record.put(0, null);
        } else {
            if (unionIndex1 == 1) {
                Object oldString0 = record.get(0);
                if (oldString0 instanceof Utf8) {
                    record.put(0, (decoder).readString(((Utf8) oldString0)));
                } else {
                    record.put(0, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'field': "+ unionIndex1));
            }
        }
        return record;
    }

}
