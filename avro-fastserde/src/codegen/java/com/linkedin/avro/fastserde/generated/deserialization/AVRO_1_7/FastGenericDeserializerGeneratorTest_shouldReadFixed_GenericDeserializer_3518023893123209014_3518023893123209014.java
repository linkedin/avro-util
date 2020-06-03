
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
    private final Schema testFixedUnion653;
    private final Schema testFixedArray656;
    private final Schema testFixedUnionArray663;
    private final Schema testFixedUnionArrayArrayElemSchema668;

    public FastGenericDeserializerGeneratorTest_shouldReadFixed_GenericDeserializer_3518023893123209014_3518023893123209014(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testFixedUnion653 = readerSchema.getField("testFixedUnion").schema();
        this.testFixedArray656 = readerSchema.getField("testFixedArray").schema();
        this.testFixedUnionArray663 = readerSchema.getField("testFixedUnionArray").schema();
        this.testFixedUnionArrayArrayElemSchema668 = testFixedUnionArray663 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed651((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed651(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadFixed;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadFixed = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadFixed = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        byte[] testFixed652;
        if ((FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0) instanceof GenericFixed)&&(((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0)).bytes().length == (2))) {
            testFixed652 = ((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(0)).bytes();
        } else {
            testFixed652 = ( new byte[2]);
        }
        decoder.readFixed(testFixed652);
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(0, new org.apache.avro.generic.GenericData.Fixed(null, testFixed652));
        int unionIndex654 = (decoder.readIndex());
        if (unionIndex654 == 0) {
            decoder.readNull();
        }
        if (unionIndex654 == 1) {
            byte[] testFixed655;
            if ((FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1) instanceof GenericFixed)&&(((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1)).bytes().length == (2))) {
                testFixed655 = ((GenericFixed) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(1)).bytes();
            } else {
                testFixed655 = ( new byte[2]);
            }
            decoder.readFixed(testFixed655);
            FastGenericDeserializerGeneratorTest_shouldReadFixed.put(1, new org.apache.avro.generic.GenericData.Fixed(null, testFixed655));
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray657 = null;
        long chunkLen658 = (decoder.readArrayStart());
        if (chunkLen658 > 0) {
            List<org.apache.avro.generic.GenericData.Fixed> testFixedArrayReuse659 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2) instanceof List) {
                testFixedArrayReuse659 = ((List) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2));
            }
            if (testFixedArrayReuse659 != (null)) {
                testFixedArrayReuse659 .clear();
                testFixedArray657 = testFixedArrayReuse659;
            } else {
                testFixedArray657 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen658), testFixedArray656);
            }
            do {
                for (int counter660 = 0; (counter660 <chunkLen658); counter660 ++) {
                    Object testFixedArrayArrayElementReuseVar661 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2) instanceof GenericArray) {
                        testFixedArrayArrayElementReuseVar661 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(2)).peek();
                    }
                    byte[] testFixed662;
                    if ((testFixedArrayArrayElementReuseVar661 instanceof GenericFixed)&&(((GenericFixed) testFixedArrayArrayElementReuseVar661).bytes().length == (2))) {
                        testFixed662 = ((GenericFixed) testFixedArrayArrayElementReuseVar661).bytes();
                    } else {
                        testFixed662 = ( new byte[2]);
                    }
                    decoder.readFixed(testFixed662);
                    testFixedArray657 .add(new org.apache.avro.generic.GenericData.Fixed(null, testFixed662));
                }
                chunkLen658 = (decoder.arrayNext());
            } while (chunkLen658 > 0);
        } else {
            testFixedArray657 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(0, testFixedArray656);
        }
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(2, testFixedArray657);
        List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArray664 = null;
        long chunkLen665 = (decoder.readArrayStart());
        if (chunkLen665 > 0) {
            List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArrayReuse666 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3) instanceof List) {
                testFixedUnionArrayReuse666 = ((List) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3));
            }
            if (testFixedUnionArrayReuse666 != (null)) {
                testFixedUnionArrayReuse666 .clear();
                testFixedUnionArray664 = testFixedUnionArrayReuse666;
            } else {
                testFixedUnionArray664 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(((int) chunkLen665), testFixedUnionArray663);
            }
            do {
                for (int counter667 = 0; (counter667 <chunkLen665); counter667 ++) {
                    Object testFixedUnionArrayArrayElementReuseVar669 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3) instanceof GenericArray) {
                        testFixedUnionArrayArrayElementReuseVar669 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadFixed.get(3)).peek();
                    }
                    int unionIndex670 = (decoder.readIndex());
                    if (unionIndex670 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex670 == 1) {
                        byte[] testFixed671;
                        if ((testFixedUnionArrayArrayElementReuseVar669 instanceof GenericFixed)&&(((GenericFixed) testFixedUnionArrayArrayElementReuseVar669).bytes().length == (2))) {
                            testFixed671 = ((GenericFixed) testFixedUnionArrayArrayElementReuseVar669).bytes();
                        } else {
                            testFixed671 = ( new byte[2]);
                        }
                        decoder.readFixed(testFixed671);
                        testFixedUnionArray664 .add(new org.apache.avro.generic.GenericData.Fixed(null, testFixed671));
                    }
                }
                chunkLen665 = (decoder.arrayNext());
            } while (chunkLen665 > 0);
        } else {
            testFixedUnionArray664 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.Fixed>(0, testFixedUnionArray663);
        }
        FastGenericDeserializerGeneratorTest_shouldReadFixed.put(3, testFixedUnionArray664);
        return FastGenericDeserializerGeneratorTest_shouldReadFixed;
    }

}
