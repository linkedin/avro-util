
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class UNION_GenericDeserializer_1971822364_1672473580
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema recordRecordSchema0;

    public UNION_GenericDeserializer_1971822364_1672473580(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordRecordSchema0 = readerSchema.getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializerecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordRecordSchema0)) {
            record0 = ((IndexedRecord)(reuse));
        } else {
            record0 = new org.apache.avro.generic.GenericData.Record(recordRecordSchema0);
        }
        record0 .put(0, (decoder.readInt()));
        return record0;
    }

}
