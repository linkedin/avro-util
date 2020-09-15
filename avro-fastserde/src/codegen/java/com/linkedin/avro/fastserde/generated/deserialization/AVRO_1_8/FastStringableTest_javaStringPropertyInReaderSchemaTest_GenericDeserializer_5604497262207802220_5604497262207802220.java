
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

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

public class FastStringableTest_javaStringPropertyInReaderSchemaTest_GenericDeserializer_5604497262207802220_5604497262207802220
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testUnionString0;
    private final Schema testStringArray0;
    private final Schema testStringMap0;

    public FastStringableTest_javaStringPropertyInReaderSchemaTest_GenericDeserializer_5604497262207802220_5604497262207802220(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testUnionString0 = readerSchema.getField("testUnionString").schema();
        this.testStringArray0 = readerSchema.getField("testStringArray").schema();
        this.testStringMap0 = readerSchema.getField("testStringMap").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastStringableTest_javaStringPropertyInReaderSchemaTest0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastStringableTest_javaStringPropertyInReaderSchemaTest0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastStringableTest_javaStringPropertyInReaderSchemaTest;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastStringableTest_javaStringPropertyInReaderSchemaTest = ((IndexedRecord)(reuse));
        } else {
            FastStringableTest_javaStringPropertyInReaderSchemaTest = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastStringableTest_javaStringPropertyInReaderSchemaTest.put(0, (decoder).readString());
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                FastStringableTest_javaStringPropertyInReaderSchemaTest.put(1, (decoder).readString());
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testUnionString': "+ unionIndex0));
        }
        List<String> testStringArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastStringableTest_javaStringPropertyInReaderSchemaTest.get(2);
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
                testStringArray1 .add((decoder).readString());
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastStringableTest_javaStringPropertyInReaderSchemaTest.put(2, testStringArray1);
        Map<String, String> testStringMap1 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            Map<String, String> testStringMapReuse0 = null;
            Object oldMap0 = FastStringableTest_javaStringPropertyInReaderSchemaTest.get(3);
            if (oldMap0 instanceof Map) {
                testStringMapReuse0 = ((Map) oldMap0);
            }
            if (testStringMapReuse0 != (null)) {
                testStringMapReuse0 .clear();
                testStringMap1 = testStringMapReuse0;
            } else {
                testStringMap1 = new HashMap<String, String>();
            }
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    String key0 = (decoder.readString());
                    testStringMap1 .put(key0, (decoder).readString());
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            testStringMap1 = Collections.emptyMap();
        }
        FastStringableTest_javaStringPropertyInReaderSchemaTest.put(3, testStringMap1);
        return FastStringableTest_javaStringPropertyInReaderSchemaTest;
    }

}
