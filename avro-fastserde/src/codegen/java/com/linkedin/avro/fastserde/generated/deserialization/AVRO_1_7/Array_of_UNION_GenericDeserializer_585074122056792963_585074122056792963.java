
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

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
    private final Schema arrayArrayElemSchema626;
    private final Schema arrayElemOptionSchema629;
    private final Schema field631;

    public Array_of_UNION_GenericDeserializer_585074122056792963_585074122056792963(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayArrayElemSchema626 = readerSchema.getElementType();
        this.arrayElemOptionSchema629 = arrayArrayElemSchema626 .getTypes().get(1);
        this.field631 = arrayElemOptionSchema629 .getField("field").schema();
    }

    public List<IndexedRecord> deserialize(List<IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        List<IndexedRecord> array622 = null;
        long chunkLen623 = (decoder.readArrayStart());
        if (chunkLen623 > 0) {
            List<IndexedRecord> arrayReuse624 = null;
            if ((reuse) instanceof List) {
                arrayReuse624 = ((List)(reuse));
            }
            if (arrayReuse624 != (null)) {
                arrayReuse624 .clear();
                array622 = arrayReuse624;
            } else {
                array622 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen623), readerSchema);
            }
            do {
                for (int counter625 = 0; (counter625 <chunkLen623); counter625 ++) {
                    Object arrayArrayElementReuseVar627 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar627 = ((GenericArray)(reuse)).peek();
                    }
                    int unionIndex628 = (decoder.readIndex());
                    if (unionIndex628 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex628 == 1) {
                        array622 .add(deserializerecord630(arrayArrayElementReuseVar627, (decoder)));
                    }
                }
                chunkLen623 = (decoder.arrayNext());
            } while (chunkLen623 > 0);
        } else {
            array622 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, readerSchema);
        }
        return array622;
    }

    public IndexedRecord deserializerecord630(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == arrayElemOptionSchema629)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(arrayElemOptionSchema629);
        }
        int unionIndex632 = (decoder.readIndex());
        if (unionIndex632 == 0) {
            decoder.readNull();
        }
        if (unionIndex632 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
