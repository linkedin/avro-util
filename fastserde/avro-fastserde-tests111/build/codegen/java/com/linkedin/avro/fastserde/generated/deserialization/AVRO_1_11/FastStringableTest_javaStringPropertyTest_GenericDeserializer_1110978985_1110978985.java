
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
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

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastStringableTest_javaStringPropertyTest0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastStringableTest_javaStringPropertyTest0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastStringableTest_javaStringPropertyTest;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastStringableTest_javaStringPropertyTest = ((IndexedRecord)(reuse));
        } else {
            FastStringableTest_javaStringPropertyTest = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        String charSequence0 = (decoder).readString();
        FastStringableTest_javaStringPropertyTest.put(0, charSequence0);
        populate_FastStringableTest_javaStringPropertyTest0((FastStringableTest_javaStringPropertyTest), (decoder));
        populate_FastStringableTest_javaStringPropertyTest1((FastStringableTest_javaStringPropertyTest), (decoder));
        return FastStringableTest_javaStringPropertyTest;
    }

    private void populate_FastStringableTest_javaStringPropertyTest0(IndexedRecord FastStringableTest_javaStringPropertyTest, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            FastStringableTest_javaStringPropertyTest.put(1, null);
        } else {
            if (unionIndex0 == 1) {
                String charSequence1 = (decoder).readString();
                FastStringableTest_javaStringPropertyTest.put(1, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testUnionString': "+ unionIndex0));
            }
        }
        List<String> testStringArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastStringableTest_javaStringPropertyTest.get(2);
        if (oldArray0 instanceof List) {
            testStringArray1 = ((List) oldArray0);
            testStringArray1 .clear();
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
        FastStringableTest_javaStringPropertyTest.put(2, testStringArray1);
    }

    private void populate_FastStringableTest_javaStringPropertyTest1(IndexedRecord FastStringableTest_javaStringPropertyTest, Decoder decoder)
        throws IOException
    {
        Map<String, String> testStringMap1 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            Map<String, String> testStringMapReuse0 = null;
            Object oldMap0 = FastStringableTest_javaStringPropertyTest.get(3);
            if (oldMap0 instanceof Map) {
                testStringMapReuse0 = ((Map) oldMap0);
            }
            if (testStringMapReuse0 != (null)) {
                testStringMapReuse0 .clear();
                testStringMap1 = testStringMapReuse0;
            } else {
                testStringMap1 = new HashMap<String, String>(((int)(((chunkLen1 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    String key0 = (decoder.readString());
                    String charSequence3 = (decoder).readString();
                    testStringMap1 .put(key0, charSequence3);
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            testStringMap1 = new HashMap<String, String>(0);
        }
        FastStringableTest_javaStringPropertyTest.put(3, testStringMap1);
    }

}
