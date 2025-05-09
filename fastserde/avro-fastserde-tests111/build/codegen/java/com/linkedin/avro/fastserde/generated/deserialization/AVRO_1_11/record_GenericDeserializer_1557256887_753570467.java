
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class record_GenericDeserializer_1557256887_753570467
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema someInts0;
    private final Schema someIntsMapSchema0;

    public record_GenericDeserializer_1557256887_753570467(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.someInts0 = readerSchema.getField("someInts").schema();
        this.someIntsMapSchema0 = someInts0 .getTypes().get(1);
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
        Map<Utf8, Integer> someInts1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            someInts1 = ((Map)(customization).getNewMapOverrideFunc().apply(record0 .get(0), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    someInts1 .put(key0, (decoder.readInt()));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            someInts1 = ((Map)(customization).getNewMapOverrideFunc().apply(record0 .get(0), 0));
        }
        record0 .put(0, someInts1);
        return record0;
    }

}
