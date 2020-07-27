
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastStringableTest_javaStringPropertyTest_GenericDeserializer_9103393295617573707_9103393295617573707
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testUnionString0;
    private final Schema testStringArray0;
    private final Schema testStringMap0;

    public FastStringableTest_javaStringPropertyTest_GenericDeserializer_9103393295617573707_9103393295617573707(Schema readerSchema) {
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
        if (FastStringableTest_javaStringPropertyTest.get(0) instanceof Utf8) {
            FastStringableTest_javaStringPropertyTest.put(0, (decoder).readString(((Utf8) FastStringableTest_javaStringPropertyTest.get(0))));
        } else {
            FastStringableTest_javaStringPropertyTest.put(0, (decoder).readString(null));
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                if (FastStringableTest_javaStringPropertyTest.get(1) instanceof Utf8) {
                    FastStringableTest_javaStringPropertyTest.put(1, (decoder).readString(((Utf8) FastStringableTest_javaStringPropertyTest.get(1))));
                } else {
                    FastStringableTest_javaStringPropertyTest.put(1, (decoder).readString(null));
                }
            }
        }
        List<Utf8> testStringArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (FastStringableTest_javaStringPropertyTest.get(2) instanceof List) {
            testStringArray1 = ((List) FastStringableTest_javaStringPropertyTest.get(2));
            testStringArray1 .clear();
        } else {
            testStringArray1 = new org.apache.avro.generic.GenericData.Array<Utf8>(((int) chunkLen0), testStringArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object testStringArrayArrayElementReuseVar0 = null;
                if (FastStringableTest_javaStringPropertyTest.get(2) instanceof GenericArray) {
                    testStringArrayArrayElementReuseVar0 = ((GenericArray) FastStringableTest_javaStringPropertyTest.get(2)).peek();
                }
                if (testStringArrayArrayElementReuseVar0 instanceof Utf8) {
                    testStringArray1 .add((decoder).readString(((Utf8) testStringArrayArrayElementReuseVar0)));
                } else {
                    testStringArray1 .add((decoder).readString(null));
                }
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastStringableTest_javaStringPropertyTest.put(2, testStringArray1);
        Map<Utf8, Utf8> testStringMap1 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            Map<Utf8, Utf8> testStringMapReuse0 = null;
            if (FastStringableTest_javaStringPropertyTest.get(3) instanceof Map) {
                testStringMapReuse0 = ((Map) FastStringableTest_javaStringPropertyTest.get(3));
            }
            if (testStringMapReuse0 != (null)) {
                testStringMapReuse0 .clear();
                testStringMap1 = testStringMapReuse0;
            } else {
                testStringMap1 = new HashMap<Utf8, Utf8>();
            }
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    testStringMap1 .put(key0, (decoder).readString(null));
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            testStringMap1 = Collections.emptyMap();
        }
        FastStringableTest_javaStringPropertyTest.put(3, testStringMap1);
        return FastStringableTest_javaStringPropertyTest;
    }

}
