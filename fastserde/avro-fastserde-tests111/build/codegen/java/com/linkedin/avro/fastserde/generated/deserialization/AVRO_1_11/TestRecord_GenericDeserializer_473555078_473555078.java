
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class TestRecord_GenericDeserializer_473555078_473555078
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema map_field0;

    public TestRecord_GenericDeserializer_473555078_473555078(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.map_field0 = readerSchema.getField("map_field").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeTestRecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeTestRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord TestRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            TestRecord = ((IndexedRecord)(reuse));
        } else {
            TestRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Map<Utf8, Utf8> map_field1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            map_field1 = ((Map)(customization).getNewMapOverrideFunc().apply(TestRecord.get(0), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    Utf8 charSequence0 = (decoder).readString(null);
                    map_field1 .put(key0, charSequence0);
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            map_field1 = ((Map)(customization).getNewMapOverrideFunc().apply(TestRecord.get(0), 0));
        }
        TestRecord.put(0, map_field1);
        return TestRecord;
    }

}
