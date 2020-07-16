
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

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
    private final Schema mapMapValueSchema0;
    private final Schema field0;

    public Map_of_record_GenericDeserializer_2141121767969292399_2141121767969292399(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapMapValueSchema0 = readerSchema.getValueType();
        this.field0 = mapMapValueSchema0 .getField("field").schema();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        Map<Utf8, IndexedRecord> map0 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            Map<Utf8, IndexedRecord> mapReuse0 = null;
            if ((reuse) instanceof Map) {
                mapReuse0 = ((Map)(reuse));
            }
            if (mapReuse0 != (null)) {
                mapReuse0 .clear();
                map0 = mapReuse0;
            } else {
                map0 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    map0 .put(key0, deserializerecord0(null, (decoder)));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            map0 = Collections.emptyMap();
        }
        return map0;
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapMapValueSchema0)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(mapMapValueSchema0);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                if (record.get(0) instanceof Utf8) {
                    record.put(0, (decoder).readString(((Utf8) record.get(0))));
                } else {
                    record.put(0, (decoder).readString(null));
                }
            }
        }
        return record;
    }

}
