
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadFixed_GenericDeserializer_8811953951139265292_8811953951139265292
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testFixedUnion799;
    private final Schema testFixedArray802;
    private final Schema testFixedUnionArray809;
    private final Schema testFixedUnionArrayArrayElemSchema814;

    public FastGenericDeserializerGeneratorTest_shouldReadFixed_GenericDeserializer_8811953951139265292_8811953951139265292(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testFixedUnion799 = readerSchema.getField("testFixedUnion").schema();
        this.testFixedArray802 = readerSchema.getField("testFixedArray").schema();
        this.testFixedUnionArray809 = readerSchema.getField("testFixedUnionArray").schema();
        this.testFixedUnionArrayArrayElemSchema814 = testFixedUnionArray809 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed797((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed797(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadFixed;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadFixed = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadFixed = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        byte[] testFixed798;
        if ((FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0) instanceof GenericFixed)&&(((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0)).bytes().length == (2))) {
            testFixed798 = ((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0)).bytes();
        } else {
            testFixed798 = ( new byte[2]);
        }
        decoder.readFixed(testFixed798);
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(0, new org.apache.avro.generic.GenericData.Fixed(testFixed798));
        int unionIndex800 = (decoder.readIndex());
        if (unionIndex800 == 0) {
            decoder.readNull();
        }
        if (unionIndex800 == 1) {
            byte[] testFixed801;
            if ((FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1) instanceof GenericFixed)&&(((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1)).bytes().length == (2))) {
                testFixed801 = ((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1)).bytes();
            } else {
                testFixed801 = ( new byte[2]);
            }
            decoder.readFixed(testFixed801);
            FastGenericDeserializerGeneratorTest_shouldReadFixed.put(1, new org.apache.avro.generic.GenericData.Fixed(testFixed801));
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray803 = null;
        long chunkLen804 = (decoder.readArrayStart());
        if (chunkLen804 > 0) {
            List<org.apache.avro.generic.GenericData.Fixed> testFixedArrayReuse805 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2) instanceof List) {
                testFixedArrayReuse805 = ((List) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2));
            }
            if (testFixedArrayReuse805 != (null)) {
                testFixedArrayReuse805 .clear();
                testFixedArray803 = testFixedArrayReuse805;
            } else {
                testFixedArray803 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen804), testFixedArray802);
            }
            do {
                for (int counter806 = 0; (counter806 <chunkLen804); counter806 ++) {
                    Object testFixedArrayArrayElementReuseVar807 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2) instanceof GenericArray) {
                        testFixedArrayArrayElementReuseVar807 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2)).peek();
                    }
                    byte[] testFixed808;
                    if ((testFixedArrayArrayElementReuseVar807 instanceof GenericFixed)&&(((GenericFixed) testFixedArrayArrayElementReuseVar807).bytes().length == (2))) {
                        testFixed808 = ((GenericFixed) testFixedArrayArrayElementReuseVar807).bytes();
                    } else {
                        testFixed808 = ( new byte[2]);
                    }
                    decoder.readFixed(testFixed808);
                    testFixedArray803 .add(new org.apache.avro.generic.GenericData.Fixed(testFixed808));
                }
                chunkLen804 = (decoder.arrayNext());
            } while (chunkLen804 > 0);
        } else {
            testFixedArray803 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(0, testFixedArray802);
        }
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(2, testFixedArray803);
        List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArray810 = null;
        long chunkLen811 = (decoder.readArrayStart());
        if (chunkLen811 > 0) {
            List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArrayReuse812 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3) instanceof List) {
                testFixedUnionArrayReuse812 = ((List) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3));
            }
            if (testFixedUnionArrayReuse812 != (null)) {
                testFixedUnionArrayReuse812 .clear();
                testFixedUnionArray810 = testFixedUnionArrayReuse812;
            } else {
                testFixedUnionArray810 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen811), testFixedUnionArray809);
            }
            do {
                for (int counter813 = 0; (counter813 <chunkLen811); counter813 ++) {
                    Object testFixedUnionArrayArrayElementReuseVar815 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3) instanceof GenericArray) {
                        testFixedUnionArrayArrayElementReuseVar815 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3)).peek();
                    }
                    int unionIndex816 = (decoder.readIndex());
                    if (unionIndex816 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex816 == 1) {
                        byte[] testFixed817;
                        if ((testFixedUnionArrayArrayElementReuseVar815 instanceof GenericFixed)&&(((GenericFixed) testFixedUnionArrayArrayElementReuseVar815).bytes().length == (2))) {
                            testFixed817 = ((GenericFixed) testFixedUnionArrayArrayElementReuseVar815).bytes();
                        } else {
                            testFixed817 = ( new byte[2]);
                        }
                        decoder.readFixed(testFixed817);
                        testFixedUnionArray810 .add(new org.apache.avro.generic.GenericData.Fixed(testFixed817));
                    }
                }
                chunkLen811 = (decoder.arrayNext());
            } while (chunkLen811 > 0);
        } else {
            testFixedUnionArray810 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(0, testFixedUnionArray809);
        }
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(3, testFixedUnionArray810);
        return FastGenericDeserializerGeneratorTest_shouldReadFixed;
    }

}
