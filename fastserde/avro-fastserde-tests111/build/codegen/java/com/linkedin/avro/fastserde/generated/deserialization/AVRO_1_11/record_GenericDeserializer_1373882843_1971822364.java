
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class record_GenericDeserializer_1373882843_1971822364
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;

    public record_GenericDeserializer_1373882843_1971822364(Schema readerSchema) {
        this.readerSchema = readerSchema;
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
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            record0 = ((IndexedRecord)(reuse));
        } else {
            record0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            record0 .put(0, (decoder.readInt()));
        } else {
            throw new RuntimeException(("Illegal union index for 'someInt': "+ unionIndex0));
        }
        return record0;
    }

}
