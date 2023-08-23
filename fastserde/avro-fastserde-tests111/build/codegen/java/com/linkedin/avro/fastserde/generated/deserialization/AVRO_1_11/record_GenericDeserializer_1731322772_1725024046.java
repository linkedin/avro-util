
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class record_GenericDeserializer_1731322772_1725024046
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final GenericData modelData;

    public record_GenericDeserializer_1731322772_1725024046(Schema readerSchema, GenericData modelData) {
        this.readerSchema = readerSchema;
        this.modelData = modelData;
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"record\",\"name\":\"record\",\"namespace\":\"com.adpilot.utils.generated.avro\",\"doc\":\"record\",\"fields\":[{\"name\":\"someInt\",\"type\":\"int\",\"doc\":\"\"}]}");
        } else {
            if (unionIndex0 == 1) {
                return deserializerecord0((reuse), (decoder));
            } else {
                throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex0));
            }
        }
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new GenericData.Record(readerSchema);
        }
        record.put(0, (decoder.readInt()));
        return record;
    }

}
