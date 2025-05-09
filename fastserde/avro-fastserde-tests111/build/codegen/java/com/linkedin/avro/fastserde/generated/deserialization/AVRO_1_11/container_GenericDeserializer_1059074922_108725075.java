
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class container_GenericDeserializer_1059074922_108725075
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema nested_maps0;
    private final Schema nested_mapsMapSchema0;
    private final Schema nested_mapsMapValueSchema0;
    private final Schema nested_mapsValueMapSchema0;
    private final Schema nested_mapsValueMapValueSchema0;

    public container_GenericDeserializer_1059074922_108725075(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.nested_maps0 = readerSchema.getField("nested_maps").schema();
        this.nested_mapsMapSchema0 = nested_maps0 .getTypes().get(1);
        this.nested_mapsMapValueSchema0 = nested_mapsMapSchema0 .getValueType();
        this.nested_mapsValueMapSchema0 = nested_mapsMapValueSchema0 .getTypes().get(1);
        this.nested_mapsValueMapValueSchema0 = nested_mapsValueMapSchema0 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializecontainer0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializecontainer0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord container0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            container0 = ((IndexedRecord)(reuse));
        } else {
            container0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Map<Utf8, Map<Utf8, Integer>> nested_maps1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            nested_maps1 = ((Map)(customization).getNewMapOverrideFunc().apply(container0 .get(0), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    Map<Utf8, Integer> nested_mapsValue0 = null;
                    long chunkLen1 = (decoder.readMapStart());
                    if (chunkLen1 > 0) {
                        nested_mapsValue0 = ((Map)(customization).getNewMapOverrideFunc().apply(null, ((int) chunkLen1)));
                        do {
                            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                                Utf8 key1 = (decoder.readString(null));
                                nested_mapsValue0 .put(key1, (decoder.readInt()));
                            }
                            chunkLen1 = (decoder.mapNext());
                        } while (chunkLen1 > 0);
                    } else {
                        nested_mapsValue0 = ((Map)(customization).getNewMapOverrideFunc().apply(null, 0));
                    }
                    nested_maps1 .put(key0, nested_mapsValue0);
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            nested_maps1 = ((Map)(customization).getNewMapOverrideFunc().apply(container0 .get(0), 0));
        }
        container0 .put(0, nested_maps1);
        return container0;
    }

}
