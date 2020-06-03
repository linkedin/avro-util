
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

public class Map_of_UNION_GenericDeserializer_2087096002965517991_2087096002965517991
    implements FastDeserializer<Map<Utf8, IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema mapMapValueSchema854;
    private final Schema mapValueOptionSchema856;
    private final Schema field858;

    public Map_of_UNION_GenericDeserializer_2087096002965517991_2087096002965517991(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapMapValueSchema854 = readerSchema.getValueType();
        this.mapValueOptionSchema856 = mapMapValueSchema854 .getTypes().get(1);
        this.field858 = mapValueOptionSchema856 .getField("field").schema();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        Map<Utf8, IndexedRecord> map849 = null;
        long chunkLen850 = (decoder.readMapStart());
        if (chunkLen850 > 0) {
            Map<Utf8, IndexedRecord> mapReuse851 = null;
            if ((reuse) instanceof Map) {
                mapReuse851 = ((Map)(reuse));
            }
            if (mapReuse851 != (null)) {
                mapReuse851 .clear();
                map849 = mapReuse851;
            } else {
                map849 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter852 = 0; (counter852 <chunkLen850); counter852 ++) {
                    Utf8 key853 = (decoder.readString(null));
                    int unionIndex855 = (decoder.readIndex());
                    if (unionIndex855 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex855 == 1) {
                        map849 .put(key853, deserializerecord857(null, (decoder)));
                    }
                }
                chunkLen850 = (decoder.mapNext());
            } while (chunkLen850 > 0);
        } else {
            map849 = Collections.emptyMap();
        }
        return map849;
    }

    public IndexedRecord deserializerecord857(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapValueOptionSchema856)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(mapValueOptionSchema856);
        }
        int unionIndex859 = (decoder.readIndex());
        if (unionIndex859 == 0) {
            decoder.readNull();
        }
        if (unionIndex859 == 1) {
            if (record.get(0) instanceof Utf8) {
                record.put(0, (decoder).readString(((Utf8) record.get(0))));
            } else {
                record.put(0, (decoder).readString(null));
            }
        }
        return record;
    }

}
