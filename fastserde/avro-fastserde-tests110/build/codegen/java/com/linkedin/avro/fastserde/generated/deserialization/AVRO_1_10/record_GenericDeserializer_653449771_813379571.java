
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_10;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class record_GenericDeserializer_653449771_813379571
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema someInts0;
    private final Schema someIntsArraySchema0;
    private final Schema someIntsArrayElemSchema0;

    public record_GenericDeserializer_653449771_813379571(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.someInts0 = readerSchema.getField("someInts").schema();
        this.someIntsArraySchema0 = someInts0 .getTypes().get(1);
        this.someIntsArrayElemSchema0 = someIntsArraySchema0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializerecord0((reuse), (decoder));
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<Integer> someInts1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = record.get(0);
        if (oldArray0 instanceof List) {
            someInts1 = ((List) oldArray0);
            someInts1 .clear();
        } else {
            someInts1 = new org.apache.avro.generic.GenericData.Array<Integer>(((int) chunkLen0), someIntsArraySchema0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                someInts1 .add((decoder.readInt()));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        record.put(0, someInts1);
        return record;
    }

}
