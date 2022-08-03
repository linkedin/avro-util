
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class record_GenericDeserializer_6705804244729881900_5919039771168630173
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema someInts0;

    public record_GenericDeserializer_6705804244729881900_5919039771168630173(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.someInts0 = readerSchema.getField("someInts").schema();
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
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"map\",\"values\":\"int\"}");
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, Integer> someIntsOption0 = null;
                long chunkLen0 = (decoder.readMapStart());
                if (chunkLen0 > 0) {
                    Map<Utf8, Integer> someIntsOptionReuse0 = null;
                    Object oldMap0 = record.get(0);
                    if (oldMap0 instanceof Map) {
                        someIntsOptionReuse0 = ((Map) oldMap0);
                    }
                    if (someIntsOptionReuse0 != (null)) {
                        someIntsOptionReuse0 .clear();
                        someIntsOption0 = someIntsOptionReuse0;
                    } else {
                        someIntsOption0 = new HashMap<Utf8, Integer>(((int)(((chunkLen0 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            int unionIndex1 = (decoder.readIndex());
                            if (unionIndex1 == 0) {
                                throw new AvroTypeException("Found \"null\", expecting \"int\"");
                            } else {
                                if (unionIndex1 == 1) {
                                    someIntsOption0 .put(key0, (decoder.readInt()));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'someIntsOptionValue': "+ unionIndex1));
                                }
                            }
                        }
                        chunkLen0 = (decoder.mapNext());
                    } while (chunkLen0 > 0);
                } else {
                    someIntsOption0 = new HashMap<Utf8, Integer>(0);
                }
                record.put(0, someIntsOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'someInts': "+ unionIndex0));
            }
        }
        return record;
    }

}
