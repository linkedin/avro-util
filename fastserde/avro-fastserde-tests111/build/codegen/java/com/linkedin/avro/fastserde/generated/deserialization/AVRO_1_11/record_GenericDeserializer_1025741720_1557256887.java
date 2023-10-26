
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

public class record_GenericDeserializer_1025741720_1557256887
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema someInts0;

    public record_GenericDeserializer_1025741720_1557256887(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.someInts0 = readerSchema.getField("someInts").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializerecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
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
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"map\",\"values\":\"int\"}");
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, Integer> someIntsOption0 = null;
                long chunkLen0 = (decoder.readMapStart());
                if (chunkLen0 > 0) {
                    someIntsOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(record.get(0), ((int) chunkLen0)));
                    do {
                        for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            int unionIndex1 = (decoder.readIndex());
                            if (unionIndex1 == 0) {
                                throw new AvroTypeException("Found \"null\", expecting \"int\"");
                            } else {
                                if (unionIndex1 == 1) {
                                    someIntsOption0 .put(key0, (decoder.readInt()));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'someIntsOptionValue': "+ unionIndex1));
                                }
                            }
                        }
                        chunkLen0 = (decoder.mapNext());
                    } while (chunkLen0 > 0);
                } else {
                    someIntsOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(record.get(0), 0));
                }
                record.put(0, someIntsOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'someInts': "+ unionIndex0));
            }
        }
        return record;
    }

}
