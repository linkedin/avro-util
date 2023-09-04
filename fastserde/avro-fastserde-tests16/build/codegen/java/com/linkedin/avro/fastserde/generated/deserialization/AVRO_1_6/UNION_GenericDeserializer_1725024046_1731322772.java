
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class UNION_GenericDeserializer_1725024046_1731322772
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema recordRecordSchema0;

    public UNION_GenericDeserializer_1725024046_1731322772(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordRecordSchema0 = readerSchema.getTypes().get(1);
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
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordRecordSchema0)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(recordRecordSchema0);
        }
        record.put(0, (decoder.readInt()));
        return record;
    }

}
