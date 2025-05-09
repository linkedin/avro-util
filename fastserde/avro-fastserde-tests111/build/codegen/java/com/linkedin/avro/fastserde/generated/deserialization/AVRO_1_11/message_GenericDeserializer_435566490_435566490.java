
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class message_GenericDeserializer_435566490_435566490
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema data0;
    private final Schema dataOptionSchema0;
    private final Schema dataOptionMapValueSchema0;

    public message_GenericDeserializer_435566490_435566490(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.data0 = readerSchema.getField("data").schema();
        this.dataOptionSchema0 = data0 .getTypes().get(1);
        this.dataOptionMapValueSchema0 = dataOptionSchema0 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializemessage0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializemessage0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord message0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            message0 = ((IndexedRecord)(reuse));
        } else {
            message0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            message0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, Map<Utf8, Integer>> dataOption0 = null;
                long chunkLen0 = (decoder.readMapStart());
                if (chunkLen0 > 0) {
                    dataOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(message0 .get(0), ((int) chunkLen0)));
                    do {
                        for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            Map<Utf8, Integer> dataOptionValue0 = null;
                            long chunkLen1 = (decoder.readMapStart());
                            if (chunkLen1 > 0) {
                                dataOptionValue0 = ((Map)(customization).getNewMapOverrideFunc().apply(null, ((int) chunkLen1)));
                                do {
                                    for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                                        Utf8 key1 = (decoder.readString(null));
                                        dataOptionValue0 .put(key1, (decoder.readInt()));
                                    }
                                    chunkLen1 = (decoder.mapNext());
                                } while (chunkLen1 > 0);
                            } else {
                                dataOptionValue0 = ((Map)(customization).getNewMapOverrideFunc().apply(null, 0));
                            }
                            dataOption0 .put(key0, dataOptionValue0);
                        }
                        chunkLen0 = (decoder.mapNext());
                    } while (chunkLen0 > 0);
                } else {
                    dataOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(message0 .get(0), 0));
                }
                message0 .put(0, dataOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'data': "+ unionIndex0));
            }
        }
        return message0;
    }

}
