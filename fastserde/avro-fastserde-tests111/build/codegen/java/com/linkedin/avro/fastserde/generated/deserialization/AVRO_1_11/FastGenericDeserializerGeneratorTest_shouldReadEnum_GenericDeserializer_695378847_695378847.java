
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadEnum_GenericDeserializer_695378847_695378847
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum0;
    private final Schema testEnumUnion0;
    private final Schema testEnumArray0;
    private final Schema testEnumUnionArray0;
    private final Schema testEnumUnionArrayArrayElemSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadEnum_GenericDeserializer_695378847_695378847(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum0 = readerSchema.getField("testEnum").schema();
        this.testEnumUnion0 = readerSchema.getField("testEnumUnion").schema();
        this.testEnumArray0 = readerSchema.getField("testEnumArray").schema();
        this.testEnumUnionArray0 = readerSchema.getField("testEnumUnionArray").schema();
        this.testEnumUnionArrayArrayElemSchema0 = testEnumUnionArray0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadEnum0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadEnum0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadEnum0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        fastGenericDeserializerGeneratorTest_shouldReadEnum0 .put(0, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
        populate_FastGenericDeserializerGeneratorTest_shouldReadEnum0((fastGenericDeserializerGeneratorTest_shouldReadEnum0), (customization), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadEnum1((fastGenericDeserializerGeneratorTest_shouldReadEnum0), (customization), (decoder));
        return fastGenericDeserializerGeneratorTest_shouldReadEnum0;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadEnum0(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadEnum0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadEnum0 .put(1, null);
        } else {
            if (unionIndex0 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadEnum0 .put(1, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
            } else {
                throw new RuntimeException(("Illegal union index for 'testEnumUnion': "+ unionIndex0));
            }
        }
        List<GenericEnumSymbol> testEnumArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = fastGenericDeserializerGeneratorTest_shouldReadEnum0 .get(2);
        if (oldArray0 instanceof List) {
            testEnumArray1 = ((List) oldArray0);
            if (testEnumArray1 instanceof GenericArray) {
                ((GenericArray) testEnumArray1).reset();
            } else {
                testEnumArray1 .clear();
            }
        } else {
            testEnumArray1 = new org.apache.avro.generic.GenericData.Array<GenericEnumSymbol>(((int) chunkLen0), testEnumArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                testEnumArray1 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        fastGenericDeserializerGeneratorTest_shouldReadEnum0 .put(2, testEnumArray1);
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadEnum1(IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadEnum0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        List<GenericEnumSymbol> testEnumUnionArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray1 = fastGenericDeserializerGeneratorTest_shouldReadEnum0 .get(3);
        if (oldArray1 instanceof List) {
            testEnumUnionArray1 = ((List) oldArray1);
            if (testEnumUnionArray1 instanceof GenericArray) {
                ((GenericArray) testEnumUnionArray1).reset();
            } else {
                testEnumUnionArray1 .clear();
            }
        } else {
            testEnumUnionArray1 = new org.apache.avro.generic.GenericData.Array<GenericEnumSymbol>(((int) chunkLen1), testEnumUnionArray0);
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testEnumUnionArrayArrayElementReuseVar0 = null;
                if (oldArray1 instanceof GenericArray) {
                    testEnumUnionArrayArrayElementReuseVar0 = ((GenericArray) oldArray1).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                if (unionIndex1 == 0) {
                    decoder.readNull();
                    testEnumUnionArray1 .add(null);
                } else {
                    if (unionIndex1 == 1) {
                        testEnumUnionArray1 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'testEnumUnionArrayElem': "+ unionIndex1));
                    }
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        fastGenericDeserializerGeneratorTest_shouldReadEnum0 .put(3, testEnumUnionArray1);
    }

}
