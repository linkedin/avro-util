
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class record_GenericDeserializer_3011431218838630619_7563779026522497792
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;

    public record_GenericDeserializer_3011431218838630619_7563779026522497792(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializerecord0((reuse), (decoder));
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            record.put(0, (decoder.readInt()));
        } else {
            if (unionIndex0 == 1) {
                throw new AvroTypeException("Found \"string\", expecting \"int\"");
            } else {
                throw new RuntimeException(("Illegal union index for 'someField': "+ unionIndex0));
            }
        }
        return record;
    }

}
