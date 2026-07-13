
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class Map_of_record_GenericDeserializer_1223705675_568621313
    implements FastDeserializer<Map<Utf8, IndexedRecord>>
{

    private final Schema readerSchema;
    private final Schema unionOptionMapValueSchema0;

    public Map_of_record_GenericDeserializer_1223705675_568621313(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.unionOptionMapValueSchema0 = readerSchema.getValueType();
    }

    public Map<Utf8, IndexedRecord> deserialize(Map<Utf8, IndexedRecord> reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"doc\":\"record\",\"fields\":[{\"name\":\"someInt\",\"type\":\"int\",\"doc\":\"\"}]}}");
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, IndexedRecord> unionOption0 = null;
                long chunkLen0 = (decoder.readMapStart());
                if (chunkLen0 > 0) {
                    unionOption0 = ((Map)(customization).getNewMapOverrideFunc().apply((reuse), ((int) chunkLen0)));
                    do {
                        for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            unionOption0 .put(key0, deserializerecord0(null, (decoder), (customization)));
                        }
                        chunkLen0 = (decoder.mapNext());
                    } while (chunkLen0 > 0);
                } else {
                    unionOption0 = ((Map)(customization).getNewMapOverrideFunc().apply((reuse), 0));
                }
                return unionOption0;
            } else {
                throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex0));
            }
        }
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == unionOptionMapValueSchema0)) {
            record0 = ((IndexedRecord)(reuse));
        } else {
            record0 = new org.apache.avro.generic.GenericData.Record(unionOptionMapValueSchema0);
        }
        record0 .put(0, (decoder.readInt()));
        return record0;
    }

}
