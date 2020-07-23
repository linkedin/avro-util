
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadFixed_GenericDeserializer_3518023893123209014_3518023893123209014
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testFixedUnion0;
    private final Schema testFixedArray0;
    private final Schema testFixedUnionArray0;
    private final Schema testFixedUnionArrayArrayElemSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadFixed_GenericDeserializer_3518023893123209014_3518023893123209014(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testFixedUnion0 = readerSchema.getField("testFixedUnion").schema();
        this.testFixedArray0 = readerSchema.getField("testFixedArray").schema();
        this.testFixedUnionArray0 = readerSchema.getField("testFixedUnionArray").schema();
        this.testFixedUnionArrayArrayElemSchema0 = testFixedUnionArray0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadFixed;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadFixed = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadFixed = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        byte[] testFixed0;
        if ((FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0) instanceof GenericFixed)&&(((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0)).bytes().length == (2))) {
            testFixed0 = ((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0)).bytes();
        } else {
            testFixed0 = ( new byte[2]);
        }
        decoder.readFixed(testFixed0);
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(0, new org.apache.avro.generic.GenericData.Fixed(null, testFixed0));
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                byte[] testFixed1;
                if ((FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1) instanceof GenericFixed)&&(((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1)).bytes().length == (2))) {
                    testFixed1 = ((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1)).bytes();
                } else {
                    testFixed1 = ( new byte[2]);
                }
                decoder.readFixed(testFixed1);
                FastGenericDeserializerGeneratorTest_shouldReadFixed.put(1, new org.apache.avro.generic.GenericData.Fixed(null, testFixed1));
            }
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2) instanceof List) {
            testFixedArray1 = ((List) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2));
            testFixedArray1 .clear();
        } else {
            testFixedArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen0), testFixedArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object testFixedArrayArrayElementReuseVar0 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2) instanceof GenericArray) {
                    testFixedArrayArrayElementReuseVar0 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2)).peek();
                }
                byte[] testFixed2;
                if ((testFixedArrayArrayElementReuseVar0 instanceof GenericFixed)&&(((GenericFixed) testFixedArrayArrayElementReuseVar0).bytes().length == (2))) {
                    testFixed2 = ((GenericFixed) testFixedArrayArrayElementReuseVar0).bytes();
                } else {
                    testFixed2 = ( new byte[2]);
                }
                decoder.readFixed(testFixed2);
                testFixedArray1 .add(new org.apache.avro.generic.GenericData.Fixed(null, testFixed2));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(2, testFixedArray1);
        List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3) instanceof List) {
            testFixedUnionArray1 = ((List) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3));
            testFixedUnionArray1 .clear();
        } else {
            testFixedUnionArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen1), testFixedUnionArray0);
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testFixedUnionArrayArrayElementReuseVar0 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3) instanceof GenericArray) {
                    testFixedUnionArrayArrayElementReuseVar0 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3)).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                if (unionIndex1 == 0) {
                    decoder.readNull();
                } else {
                    if (unionIndex1 == 1) {
                        byte[] testFixed3;
                        if ((testFixedUnionArrayArrayElementReuseVar0 instanceof GenericFixed)&&(((GenericFixed) testFixedUnionArrayArrayElementReuseVar0).bytes().length == (2))) {
                            testFixed3 = ((GenericFixed) testFixedUnionArrayArrayElementReuseVar0).bytes();
                        } else {
                            testFixed3 = ( new byte[2]);
                        }
                        decoder.readFixed(testFixed3);
                        testFixedUnionArray1 .add(new org.apache.avro.generic.GenericData.Fixed(null, testFixed3));
                    }
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(3, testFixedUnionArray1);
        return FastGenericDeserializerGeneratorTest_shouldReadFixed;
    }

}
