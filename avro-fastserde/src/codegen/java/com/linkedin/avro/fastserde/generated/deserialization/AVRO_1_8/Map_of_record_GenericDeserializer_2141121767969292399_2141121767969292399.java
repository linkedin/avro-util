
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

public class Map_of_record_GenericDeserializer_2141121767969292399_2141121767969292399
    implements FastDeserializer<Map<Utf8, IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema mapMapValueSchema699;
    private final Schema field701;

    public Map_of_record_GenericDeserializer_2141121767969292399_2141121767969292399(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapMapValueSchema699 = readerSchema.getValueType();
        this.field701 = mapMapValueSchema699 .getField("field").schema();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        Map<Utf8, IndexedRecord> map694 = null;
        long chunkLen695 = (decoder.readMapStart());
        if (chunkLen695 > 0) {
            Map<Utf8, IndexedRecord> mapReuse696 = null;
            if ((reuse) instanceof Map) {
                mapReuse696 = ((Map)(reuse));
            }
            if (mapReuse696 != (null)) {
                mapReuse696 .clear();
                map694 = mapReuse696;
            } else {
                map694 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter697 = 0; (counter697 <chunkLen695); counter697 ++) {
                    Utf8 key698 = (decoder.readString(null));
                    map694 .put(key698, deserializerecord700(null, (decoder)));
                }
                chunkLen695 = (decoder.mapNext());
            } while (chunkLen695 > 0);
        } else {
            map694 = Collections.emptyMap();
        }
        return map694;
    }

    public IndexedRecord deserializerecord700(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapMapValueSchema699)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(mapMapValueSchema699);
        }
        int unionIndex702 = (decoder.readIndex());
        if (unionIndex702 == 0) {
            decoder.readNull();
        }
        if (unionIndex702 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
