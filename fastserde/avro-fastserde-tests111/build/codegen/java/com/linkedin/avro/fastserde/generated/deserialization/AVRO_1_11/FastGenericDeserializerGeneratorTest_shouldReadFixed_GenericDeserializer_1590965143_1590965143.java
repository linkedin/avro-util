
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadFixed_GenericDeserializer_1590965143_1590965143
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testFixed0;
    private final Schema testFixedUnion0;
    private final Schema testFixedArray0;
    private final Schema testFixedUnionArray0;
    private final Schema testFixedUnionArrayArrayElemSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadFixed_GenericDeserializer_1590965143_1590965143(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testFixed0 = readerSchema.getField("testFixed").schema();
        this.testFixedUnion0 = readerSchema.getField("testFixedUnion").schema();
        this.testFixedArray0 = readerSchema.getField("testFixedArray").schema();
        this.testFixedUnionArray0 = readerSchema.getField("testFixedUnionArray").schema();
        this.testFixedUnionArrayArrayElemSchema0 = testFixedUnionArray0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadFixed0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadFixed0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadFixed0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadFixed0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        byte[] testFixed1;
        Object oldFixed0 = fastGenericDeserializerGeneratorTest_shouldReadFixed0 .get(0);
        if ((oldFixed0 instanceof GenericFixed)&&(((GenericFixed) oldFixed0).bytes().length == (2))) {
            testFixed1 = ((GenericFixed) oldFixed0).bytes();
        } else {
            testFixed1 = ( new byte[2]);
        }
        decoder.readFixed(testFixed1);
        fastGenericDeserializerGeneratorTest_shouldReadFixed0 .put(0, new org.apache.avro.generic.GenericData.Fixed(testFixed0, testFixed1));
        populate_FastGenericDeserializerGeneratorTest_shouldReadFixed0((fastGenericDeserializerGeneratorTest_shouldReadFixed0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadFixed1((fastGenericDeserializerGeneratorTest_shouldReadFixed0), (customization), (decoder));
        return fastGenericDeserializerGeneratorTest_shouldReadFixed0;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadFixed0(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadFixed0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadFixed0 .put(1, null);
        } else {
            if (unionIndex0 == 1) {
                byte[] testFixed2;
                Object oldFixed1 = fastGenericDeserializerGeneratorTest_shouldReadFixed0 .get(1);
                if ((oldFixed1 instanceof GenericFixed)&&(((GenericFixed) oldFixed1).bytes().length == (2))) {
                    testFixed2 = ((GenericFixed) oldFixed1).bytes();
                } else {
                    testFixed2 = ( new byte[2]);
                }
                decoder.readFixed(testFixed2);
                fastGenericDeserializerGeneratorTest_shouldReadFixed0 .put(1, new org.apache.avro.generic.GenericData.Fixed(testFixed0, testFixed2));
            } else {
                throw new RuntimeException(("Illegal union index for 'testFixedUnion': "+ unionIndex0));
            }
        }
        List<GenericFixed> testFixedArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = fastGenericDeserializerGeneratorTest_shouldReadFixed0 .get(2);
        if (oldArray0 instanceof List) {
            testFixedArray1 = ((List) oldArray0);
            if (testFixedArray1 instanceof GenericArray) {
                ((GenericArray) testFixedArray1).reset();
            } else {
                testFixedArray1 .clear();
            }
        } else {
            testFixedArray1 = new org.apache.avro.generic.GenericData.Array<GenericFixed>(((int) chunkLen0), testFixedArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object testFixedArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    testFixedArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                byte[] testFixed3;
                Object oldFixed2 = testFixedArrayArrayElementReuseVar0;
                if ((oldFixed2 instanceof GenericFixed)&&(((GenericFixed) oldFixed2).bytes().length == (2))) {
                    testFixed3 = ((GenericFixed) oldFixed2).bytes();
                } else {
                    testFixed3 = ( new byte[2]);
                }
                decoder.readFixed(testFixed3);
                testFixedArray1 .add(new org.apache.avro.generic.GenericData.Fixed(testFixed0, testFixed3));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        fastGenericDeserializerGeneratorTest_shouldReadFixed0 .put(2, testFixedArray1);
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadFixed1(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadFixed0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        List<GenericFixed> testFixedUnionArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray1 = fastGenericDeserializerGeneratorTest_shouldReadFixed0 .get(3);
        if (oldArray1 instanceof List) {
            testFixedUnionArray1 = ((List) oldArray1);
            if (testFixedUnionArray1 instanceof GenericArray) {
                ((GenericArray) testFixedUnionArray1).reset();
            } else {
                testFixedUnionArray1 .clear();
            }
        } else {
            testFixedUnionArray1 = new org.apache.avro.generic.GenericData.Array<GenericFixed>(((int) chunkLen1), testFixedUnionArray0);
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testFixedUnionArrayArrayElementReuseVar0 = null;
                if (oldArray1 instanceof GenericArray) {
                    testFixedUnionArrayArrayElementReuseVar0 = ((GenericArray) oldArray1).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                if (unionIndex1 == 0) {
                    decoder.readNull();
                    testFixedUnionArray1 .add(null);
                } else {
                    if (unionIndex1 == 1) {
                        byte[] testFixed4;
                        Object oldFixed3 = testFixedUnionArrayArrayElementReuseVar0;
                        if ((oldFixed3 instanceof GenericFixed)&&(((GenericFixed) oldFixed3).bytes().length == (2))) {
                            testFixed4 = ((GenericFixed) oldFixed3).bytes();
                        } else {
                            testFixed4 = ( new byte[2]);
                        }
                        decoder.readFixed(testFixed4);
                        testFixedUnionArray1 .add(new org.apache.avro.generic.GenericData.Fixed(testFixed0, testFixed4));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'testFixedUnionArrayElem': "+ unionIndex1));
                    }
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        fastGenericDeserializerGeneratorTest_shouldReadFixed0 .put(3, testFixedUnionArray1);
    }

}
