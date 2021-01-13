
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_10;

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
    private final Schema mapMapValueSchema0;
    private final Schema mapValueOptionSchema0;
    private final Schema field0;

    public Map_of_UNION_GenericDeserializer_2087096002965517991_2087096002965517991(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapMapValueSchema0 = readerSchema.getValueType();
        this.mapValueOptionSchema0 = mapMapValueSchema0 .getTypes().get(1);
        this.field0 = mapValueOptionSchema0 .getField("field").schema();
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
                map0 = new HashMap<Utf8, IndexedRecord>(((int)(((chunkLen0 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    int unionIndex0 = (decoder.readIndex());
                    switch (unionIndex0) {
                        case  0 :
                            decoder.readNull();
                            break;
                        case  1 :
                            map0 .put(key0, deserializerecord0(null, (decoder)));
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'mapValue': "+ unionIndex0));
                    }
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
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapValueOptionSchema0)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(mapValueOptionSchema0);
        }
        int unionIndex1 = (decoder.readIndex());
        switch (unionIndex1) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString0 = record.get(0);
                if (oldString0 instanceof Utf8) {
                    record.put(0, (decoder).readString(((Utf8) oldString0)));
                } else {
                    record.put(0, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'field': "+ unionIndex1));
        }
        return record;
    }

}
