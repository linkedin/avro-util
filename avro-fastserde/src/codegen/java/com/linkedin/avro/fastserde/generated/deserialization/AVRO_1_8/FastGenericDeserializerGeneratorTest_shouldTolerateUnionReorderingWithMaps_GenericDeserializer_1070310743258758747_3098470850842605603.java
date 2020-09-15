
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps_GenericDeserializer_1070310743258758747_3098470850842605603
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;
    private final Schema testOptionSchema0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps_GenericDeserializer_1070310743258758747_3098470850842605603(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
        this.testOptionSchema0 = test0 .getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
            {
                Map<Utf8, Integer> testOption0 = null;
                long chunkLen0 = (decoder.readMapStart());
                if (chunkLen0 > 0) {
                    Map<Utf8, Integer> testOptionReuse0 = null;
                    Object oldMap0 = FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps.get(0);
                    if (oldMap0 instanceof Map) {
                        testOptionReuse0 = ((Map) oldMap0);
                    }
                    if (testOptionReuse0 != (null)) {
                        testOptionReuse0 .clear();
                        testOption0 = testOptionReuse0;
                    } else {
                        testOption0 = new HashMap<Utf8, Integer>();
                    }
                    do {
                        for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            testOption0 .put(key0, (decoder.readInt()));
                        }
                        chunkLen0 = (decoder.mapNext());
                    } while (chunkLen0 > 0);
                } else {
                    testOption0 = Collections.emptyMap();
                }
                FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps.put(0, testOption0);
                break;
            }
            case  1 :
                decoder.readNull();
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
        }
        return FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithMaps;
    }

}
