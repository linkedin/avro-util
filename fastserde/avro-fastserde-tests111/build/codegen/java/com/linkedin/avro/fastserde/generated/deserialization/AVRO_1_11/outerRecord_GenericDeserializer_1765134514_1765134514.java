
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class outerRecord_GenericDeserializer_1765134514_1765134514
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema mapField0;
    private final Schema mapFieldMapValueSchema0;
    private final Schema internalMapField0;

    public outerRecord_GenericDeserializer_1765134514_1765134514(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.mapField0 = readerSchema.getField("mapField").schema();
        this.mapFieldMapValueSchema0 = mapField0 .getValueType();
        this.internalMapField0 = mapFieldMapValueSchema0 .getField("internalMapField").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeouterRecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeouterRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord outerRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            outerRecord0 = ((IndexedRecord)(reuse));
        } else {
            outerRecord0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        Map<Utf8, IndexedRecord> mapField1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            mapField1 = ((Map)(customization).getNewMapOverrideFunc().apply(outerRecord0 .get(0), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    mapField1 .put(key0, deserializeinnerRecord0(null, (decoder), (customization)));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            mapField1 = ((Map)(customization).getNewMapOverrideFunc().apply(outerRecord0 .get(0), 0));
        }
        outerRecord0 .put(0, mapField1);
        return outerRecord0;
    }

    public IndexedRecord deserializeinnerRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord innerRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == mapFieldMapValueSchema0)) {
            innerRecord0 = ((IndexedRecord)(reuse));
        } else {
            innerRecord0 = new org.apache.avro.generic.GenericData.Record(mapFieldMapValueSchema0);
        }
        Map<Utf8, Long> internalMapField1 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            internalMapField1 = ((Map)(customization).getNewMapOverrideFunc().apply(innerRecord0 .get(0), ((int) chunkLen1)));
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    Utf8 key1 = (decoder.readString(null));
                    internalMapField1 .put(key1, (decoder.readLong()));
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            internalMapField1 = ((Map)(customization).getNewMapOverrideFunc().apply(innerRecord0 .get(0), 0));
        }
        innerRecord0 .put(0, internalMapField1);
        return innerRecord0;
    }

}
