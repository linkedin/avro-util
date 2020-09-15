
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

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
        Object oldFixed0 = FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0);
        if ((oldFixed0 instanceof GenericFixed)&&(((GenericFixed) oldFixed0).bytes().length == (2))) {
            testFixed0 = ((GenericFixed) oldFixed0).bytes();
        } else {
            testFixed0 = ( new byte[2]);
        }
        decoder.readFixed(testFixed0);
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(0, new org.apache.avro.generic.GenericData.Fixed(null, testFixed0));
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                byte[] testFixed1;
                Object oldFixed1 = FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1);
                if ((oldFixed1 instanceof GenericFixed)&&(((GenericFixed) oldFixed1).bytes().length == (2))) {
                    testFixed1 = ((GenericFixed) oldFixed1).bytes();
                } else {
                    testFixed1 = ( new byte[2]);
                }
                decoder.readFixed(testFixed1);
                FastGenericDeserializerGeneratorTest_shouldReadFixed.put(1, new org.apache.avro.generic.GenericData.Fixed(null, testFixed1));
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testFixedUnion': "+ unionIndex0));
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2);
        if (oldArray0 instanceof List) {
            testFixedArray1 = ((List) oldArray0);
            testFixedArray1 .clear();
        } else {
            testFixedArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen0), testFixedArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object testFixedArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    testFixedArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                byte[] testFixed2;
                Object oldFixed2 = testFixedArrayArrayElementReuseVar0;
                if ((oldFixed2 instanceof GenericFixed)&&(((GenericFixed) oldFixed2).bytes().length == (2))) {
                    testFixed2 = ((GenericFixed) oldFixed2).bytes();
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
        Object oldArray1 = FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3);
        if (oldArray1 instanceof List) {
            testFixedUnionArray1 = ((List) oldArray1);
            testFixedUnionArray1 .clear();
        } else {
            testFixedUnionArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen1), testFixedUnionArray0);
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testFixedUnionArrayArrayElementReuseVar0 = null;
                if (oldArray1 instanceof GenericArray) {
                    testFixedUnionArrayArrayElementReuseVar0 = ((GenericArray) oldArray1).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                switch (unionIndex1) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                    {
                        byte[] testFixed3;
                        Object oldFixed3 = testFixedUnionArrayArrayElementReuseVar0;
                        if ((oldFixed3 instanceof GenericFixed)&&(((GenericFixed) oldFixed3).bytes().length == (2))) {
                            testFixed3 = ((GenericFixed) oldFixed3).bytes();
                        } else {
                            testFixed3 = ( new byte[2]);
                        }
                        decoder.readFixed(testFixed3);
                        testFixedUnionArray1 .add(new org.apache.avro.generic.GenericData.Fixed(null, testFixed3));
                        break;
                    }
                    default:
                        throw new RuntimeException(("Illegal union index for 'testFixedUnionArrayElem': "+ unionIndex1));
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(3, testFixedUnionArray1);
        return FastGenericDeserializerGeneratorTest_shouldReadFixed;
    }

}
