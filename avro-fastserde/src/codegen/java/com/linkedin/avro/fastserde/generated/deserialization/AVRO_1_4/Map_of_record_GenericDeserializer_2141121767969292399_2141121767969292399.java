
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

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
    private final Schema mapMapValueSchema845;
    private final Schema field847;

    public Map_of_record_GenericDeserializer_2141121767969292399_2141121767969292399(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapMapValueSchema845 = readerSchema.getValueType();
        this.field847 = mapMapValueSchema845 .getField("field").schema();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        Map<Utf8, IndexedRecord> map840 = null;
        long chunkLen841 = (decoder.readMapStart());
        if (chunkLen841 > 0) {
            Map<Utf8, IndexedRecord> mapReuse842 = null;
            if ((reuse) instanceof Map) {
                mapReuse842 = ((Map)(reuse));
            }
            if (mapReuse842 != (null)) {
                mapReuse842 .clear();
                map840 = mapReuse842;
            } else {
                map840 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter843 = 0; (counter843 <chunkLen841); counter843 ++) {
                    Utf8 key844 = (decoder.readString(null));
                    map840 .put(key844, deserializerecord846(null, (decoder)));
                }
                chunkLen841 = (decoder.mapNext());
            } while (chunkLen841 > 0);
        } else {
            map840 = Collections.emptyMap();
        }
        return map840;
    }

    public IndexedRecord deserializerecord846(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapMapValueSchema845)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(mapMapValueSchema845);
        }
        int unionIndex848 = (decoder.readIndex());
        if (unionIndex848 == 0) {
            decoder.readNull();
        }
        if (unionIndex848 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
