
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Map_of_record_GenericDeserializer_4807922374121811220_3052180383482420701
    implements FastDeserializer<Map<Utf8, IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema unionOptionMapValueSchema0;

    public Map_of_record_GenericDeserializer_4807922374121811220_3052180383482420701(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.unionOptionMapValueSchema0 = readerSchema.getValueType();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"com.adpilot.utils.generated.avro\",\"doc\":\"record\",\"fields\":[{\"name\":\"someInt\",\"type\":\"int\",\"doc\":\"\"}]}}");
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, IndexedRecord> unionOption0 = null;
                long chunkLen0 = (decoder.readMapStart());
                if (chunkLen0 > 0) {
                    Map<Utf8, IndexedRecord> unionOptionReuse0 = null;
                    if ((reuse) instanceof Map) {
                        unionOptionReuse0 = ((Map)(reuse));
                    }
                    if (unionOptionReuse0 != (null)) {
                        unionOptionReuse0 .clear();
                        unionOption0 = unionOptionReuse0;
                    } else {
                        unionOption0 = new HashMap<Utf8, IndexedRecord>(((int)(((chunkLen0 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            unionOption0 .put(key0, deserializerecord0(null, (decoder)));
                        }
                        chunkLen0 = (decoder.mapNext());
                    } while (chunkLen0 > 0);
                } else {
                    unionOption0 = new HashMap<Utf8, IndexedRecord>(0);
                }
                return unionOption0;
            } else {
                throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex0));
            }
        }
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == unionOptionMapValueSchema0)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(unionOptionMapValueSchema0);
        }
        record.put(0, (decoder.readInt()));
        return record;
    }

}
