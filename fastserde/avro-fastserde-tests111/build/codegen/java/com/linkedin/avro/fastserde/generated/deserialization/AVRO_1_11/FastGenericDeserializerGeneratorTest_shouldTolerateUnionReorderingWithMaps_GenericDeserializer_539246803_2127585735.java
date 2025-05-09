
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps_GenericDeserializer_539246803_2127585735
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;
    private final Schema testOptionSchema0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps_GenericDeserializer_539246803_2127585735(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
        this.testOptionSchema0 = test0 .getTypes().get(0);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, Integer> testOption0 = null;
                long chunkLen0 = (decoder.readMapStart());
                if (chunkLen0 > 0) {
                    testOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0 .get(0), ((int) chunkLen0)));
                    do {
                        for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            testOption0 .put(key0, (decoder.readInt()));
                        }
                        chunkLen0 = (decoder.mapNext());
                    } while (chunkLen0 > 0);
                } else {
                    testOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0 .get(0), 0));
                }
                fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0 .put(0, testOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
            }
        }
        return fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0;
    }

}
