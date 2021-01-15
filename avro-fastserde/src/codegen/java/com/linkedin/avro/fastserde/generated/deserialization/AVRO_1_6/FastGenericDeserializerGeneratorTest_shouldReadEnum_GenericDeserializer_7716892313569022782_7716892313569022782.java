
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadEnum_GenericDeserializer_7716892313569022782_7716892313569022782
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum0;
    private final Schema testEnumUnion0;
    private final Schema testEnumArray0;
    private final Schema testEnumUnionArray0;
    private final Schema testEnumUnionArrayArrayElemSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadEnum_GenericDeserializer_7716892313569022782_7716892313569022782(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum0 = readerSchema.getField("testEnum").schema();
        this.testEnumUnion0 = readerSchema.getField("testEnumUnion").schema();
        this.testEnumArray0 = readerSchema.getField("testEnumArray").schema();
        this.testEnumUnionArray0 = readerSchema.getField("testEnumUnionArray").schema();
        this.testEnumUnionArrayArrayElemSchema0 = testEnumUnionArray0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadEnum;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadEnum = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadEnum = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(0, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                FastGenericDeserializerGeneratorTest_shouldReadEnum.put(1, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testEnumUnion': "+ unionIndex0));
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2);
        if (oldArray0 instanceof List) {
            testEnumArray1 = ((List) oldArray0);
            testEnumArray1 .clear();
        } else {
            testEnumArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen0), testEnumArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                testEnumArray1 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(2, testEnumArray1);
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray1 = FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3);
        if (oldArray1 instanceof List) {
            testEnumUnionArray1 = ((List) oldArray1);
            testEnumUnionArray1 .clear();
        } else {
            testEnumUnionArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen1), testEnumUnionArray0);
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testEnumUnionArrayArrayElementReuseVar0 = null;
                if (oldArray1 instanceof GenericArray) {
                    testEnumUnionArrayArrayElementReuseVar0 = ((GenericArray) oldArray1).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                switch (unionIndex1) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        testEnumUnionArray1 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get((decoder.readEnum()))));
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'testEnumUnionArrayElem': "+ unionIndex1));
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(3, testEnumUnionArray1);
        return FastGenericDeserializerGeneratorTest_shouldReadEnum;
    }

}
