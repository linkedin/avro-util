
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Map_of_UNION_GenericDeserializer_2087096002965517991_2087096002965517991
    implements FastDeserializer<Map<Utf8, IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema mapMapValueSchema708;
    private final Schema mapValueOptionSchema710;
    private final Schema field712;

    public Map_of_UNION_GenericDeserializer_2087096002965517991_2087096002965517991(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapMapValueSchema708 = readerSchema.getValueType();
        this.mapValueOptionSchema710 = mapMapValueSchema708 .getTypes().get(1);
        this.field712 = mapValueOptionSchema710 .getField("field").schema();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        Map<Utf8, IndexedRecord> map703 = null;
        long chunkLen704 = (decoder.readMapStart());
        if (chunkLen704 > 0) {
            Map<Utf8, IndexedRecord> mapReuse705 = null;
            if ((reuse) instanceof Map) {
                mapReuse705 = ((Map)(reuse));
            }
            if (mapReuse705 != (null)) {
                mapReuse705 .clear();
                map703 = mapReuse705;
            } else {
                map703 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter706 = 0; (counter706 <chunkLen704); counter706 ++) {
                    Utf8 key707 = (decoder.readString(null));
                    int unionIndex709 = (decoder.readIndex());
                    if (unionIndex709 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex709 == 1) {
                        map703 .put(key707, deserializerecord711(null, (decoder)));
                    }
                }
                chunkLen704 = (decoder.mapNext());
            } while (chunkLen704 > 0);
        } else {
            map703 = Collections.emptyMap();
        }
        return map703;
    }

    public IndexedRecord deserializerecord711(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapValueOptionSchema710)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(mapValueOptionSchema710);
        }
        int unionIndex713 = (decoder.readIndex());
        if (unionIndex713 == 0) {
            decoder.readNull();
        }
        if (unionIndex713 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
