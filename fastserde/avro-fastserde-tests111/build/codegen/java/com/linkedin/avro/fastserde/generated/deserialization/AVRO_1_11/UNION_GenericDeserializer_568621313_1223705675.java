
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class UNION_GenericDeserializer_568621313_1223705675
    implements FastDeserializer<Map<Utf8, IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema mapMapSchema0;
    private final Schema mapMapValueSchema0;

    public UNION_GenericDeserializer_568621313_1223705675(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapMapSchema0 = readerSchema.getTypes().get(1);
        this.mapMapValueSchema0 = mapMapSchema0 .getValueType();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        Map<Utf8, IndexedRecord> map0 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            map0 = ((Map)(customization).getNewMapOverrideFunc().apply((reuse), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    map0 .put(key0, deserializerecord0(null, (decoder), (customization)));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            map0 = ((Map)(customization).getNewMapOverrideFunc().apply((reuse), 0));
        }
        return map0;
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapMapValueSchema0)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(mapMapValueSchema0);
        }
        record.put(0, (decoder.readInt()));
        return record;
    }

}
