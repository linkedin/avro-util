
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastStringableTest_javaStringPropertyTest_GenericDeserializer_1110978985_1110978985
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testUnionString0;
    private final Schema testStringArray0;
    private final Schema testStringMap0;

    public FastStringableTest_javaStringPropertyTest_GenericDeserializer_1110978985_1110978985(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testUnionString0 = readerSchema.getField("testUnionString").schema();
        this.testStringArray0 = readerSchema.getField("testStringArray").schema();
        this.testStringMap0 = readerSchema.getField("testStringMap").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastStringableTest_javaStringPropertyTest0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastStringableTest_javaStringPropertyTest0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastStringableTest_javaStringPropertyTest0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastStringableTest_javaStringPropertyTest0 = ((IndexedRecord)(reuse));
        } else {
            fastStringableTest_javaStringPropertyTest0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        String charSequence0 = (decoder).readString();
        fastStringableTest_javaStringPropertyTest0 .put(0, charSequence0);
        populate_FastStringableTest_javaStringPropertyTest0((fastStringableTest_javaStringPropertyTest0), (customization), (decoder));
        populate_FastStringableTest_javaStringPropertyTest1((fastStringableTest_javaStringPropertyTest0), (customization), (decoder));
        return fastStringableTest_javaStringPropertyTest0;
    }

    private void populate_FastStringableTest_javaStringPropertyTest0(IndexedRecord fastStringableTest_javaStringPropertyTest0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastStringableTest_javaStringPropertyTest0 .put(1, null);
        } else {
            if (unionIndex0 == 1) {
                String charSequence1 = (decoder).readString();
                fastStringableTest_javaStringPropertyTest0 .put(1, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testUnionString': "+ unionIndex0));
            }
        }
        List<String> testStringArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = fastStringableTest_javaStringPropertyTest0 .get(2);
        if (oldArray0 instanceof List) {
            testStringArray1 = ((List) oldArray0);
            if (testStringArray1 instanceof GenericArray) {
                ((GenericArray) testStringArray1).reset();
            } else {
                testStringArray1 .clear();
            }
        } else {
            testStringArray1 = new org.apache.avro.generic.GenericData.Array<String>(((int) chunkLen0), testStringArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object testStringArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    testStringArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                String charSequence2 = (decoder).readString();
                testStringArray1 .add(charSequence2);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        fastStringableTest_javaStringPropertyTest0 .put(2, testStringArray1);
    }

    private void populate_FastStringableTest_javaStringPropertyTest1(IndexedRecord fastStringableTest_javaStringPropertyTest0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        Map<String, String> testStringMap1 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            testStringMap1 = ((Map)(customization).getNewMapOverrideFunc().apply(fastStringableTest_javaStringPropertyTest0 .get(3), ((int) chunkLen1)));
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    String key0 = (decoder.readString());
                    String charSequence3 = (decoder).readString();
                    testStringMap1 .put(key0, charSequence3);
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            testStringMap1 = ((Map)(customization).getNewMapOverrideFunc().apply(fastStringableTest_javaStringPropertyTest0 .get(3), 0));
        }
        fastStringableTest_javaStringPropertyTest0 .put(3, testStringMap1);
    }

}
