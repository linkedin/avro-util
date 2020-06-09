
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Array_of_UNION_GenericDeserializer_585074122056792963_585074122056792963
    implements FastDeserializer<List<IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema arrayArrayElemSchema0;
    private final Schema arrayElemOptionSchema0;
    private final Schema field0;

    public Array_of_UNION_GenericDeserializer_585074122056792963_585074122056792963(Schema readerSchema) {
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
        if (chunkLen0 > 0) {
            List<IndexedRecord> arrayReuse0 = null;
            if ((reuse) instanceof List) {
                arrayReuse0 = ((List)(reuse));
            }
            if (arrayReuse0 != (null)) {
                arrayReuse0 .clear();
                array0 = arrayReuse0;
            } else {
                array0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), readerSchema);
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Object arrayArrayElementReuseVar0 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                    }
                    int unionIndex0 = (decoder.readIndex());
                    if (unionIndex0 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex0 == 1) {
                        array0 .add(deserializerecord0(arrayArrayElementReuseVar0, (decoder)));
                    }
                }
                chunkLen0 = (decoder.arrayNext());
            } while (chunkLen0 > 0);
        } else {
            array0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, readerSchema);
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
        }
        if (unionIndex1 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
