
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps_GenericDeserializer_2127585735_539246803
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;
    private final Schema testOptionSchema0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps_GenericDeserializer_2127585735_539246803(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
        this.testOptionSchema0 = test0 .getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            Map<Utf8, Integer> testOption0 = null;
            long chunkLen0 = (decoder.readMapStart());
            if (chunkLen0 > 0) {
                testOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps.get(0), ((int) chunkLen0)));
                do {
                    for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                        Utf8 key0 = (decoder.readString(null));
                        testOption0 .put(key0, (decoder.readInt()));
                    }
                    chunkLen0 = (decoder.mapNext());
                } while (chunkLen0 > 0);
            } else {
                testOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps.get(0), 0));
            }
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps.put(0, testOption0);
        } else {
            if (unionIndex0 == 1) {
                decoder.readNull();
                FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps.put(0, null);
            } else {
                throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps;
    }

}
