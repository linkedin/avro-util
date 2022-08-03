
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class record_GenericDeserializer_5919039771168630173_6705804244729881900
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema someInts0;
    private final Schema someIntsMapSchema0;
    private final Schema someIntsMapValueSchema0;

    public record_GenericDeserializer_5919039771168630173_6705804244729881900(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.someInts0 = readerSchema.getField("someInts").schema();
        this.someIntsMapSchema0 = someInts0 .getTypes().get(1);
        this.someIntsMapValueSchema0 = someIntsMapSchema0 .getValueType();
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
        Map<Utf8, Integer> someInts1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            Map<Utf8, Integer> someIntsReuse0 = null;
            Object oldMap0 = record.get(0);
            if (oldMap0 instanceof Map) {
                someIntsReuse0 = ((Map) oldMap0);
            }
            if (someIntsReuse0 != (null)) {
                someIntsReuse0 .clear();
                someInts1 = someIntsReuse0;
            } else {
                someInts1 = new HashMap<Utf8, Integer>(((int)(((chunkLen0 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    someInts1 .put(key0, (decoder.readInt()));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            someInts1 = new HashMap<Utf8, Integer>(0);
        }
        record.put(0, someInts1);
        return record;
    }

}
