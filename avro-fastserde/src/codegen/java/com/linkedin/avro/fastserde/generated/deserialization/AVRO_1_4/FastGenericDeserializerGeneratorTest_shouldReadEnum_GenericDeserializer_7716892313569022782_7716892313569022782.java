
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

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
    private final Schema testEnum780;
    private final Schema testEnumUnion781;
    private final Schema testEnumArray783;
    private final Schema testEnumUnionArray789;
    private final Schema testEnumUnionArrayArrayElemSchema794;

    public FastGenericDeserializerGeneratorTest_shouldReadEnum_GenericDeserializer_7716892313569022782_7716892313569022782(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum780 = readerSchema.getField("testEnum").schema();
        this.testEnumUnion781 = readerSchema.getField("testEnumUnion").schema();
        this.testEnumArray783 = readerSchema.getField("testEnumArray").schema();
        this.testEnumUnionArray789 = readerSchema.getField("testEnumUnionArray").schema();
        this.testEnumUnionArrayArrayElemSchema794 = testEnumUnionArray789 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum779((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum779(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadEnum;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadEnum = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadEnum = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(0, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum780 .getEnumSymbols().get((decoder.readEnum()))));
        int unionIndex782 = (decoder.readIndex());
        if (unionIndex782 == 0) {
            decoder.readNull();
        }
        if (unionIndex782 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadEnum.put(1, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum780 .getEnumSymbols().get((decoder.readEnum()))));
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray784 = null;
        long chunkLen785 = (decoder.readArrayStart());
        if (chunkLen785 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArrayReuse786 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2) instanceof List) {
                testEnumArrayReuse786 = ((List) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2));
            }
            if (testEnumArrayReuse786 != (null)) {
                testEnumArrayReuse786 .clear();
                testEnumArray784 = testEnumArrayReuse786;
            } else {
                testEnumArray784 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen785), testEnumArray783);
            }
            do {
                for (int counter787 = 0; (counter787 <chunkLen785); counter787 ++) {
                    Object testEnumArrayArrayElementReuseVar788 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2) instanceof GenericArray) {
                        testEnumArrayArrayElementReuseVar788 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2)).peek();
                    }
                    testEnumArray784 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum780 .getEnumSymbols().get((decoder.readEnum()))));
                }
                chunkLen785 = (decoder.arrayNext());
            } while (chunkLen785 > 0);
        } else {
            testEnumArray784 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumArray783);
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(2, testEnumArray784);
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray790 = null;
        long chunkLen791 = (decoder.readArrayStart());
        if (chunkLen791 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArrayReuse792 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3) instanceof List) {
                testEnumUnionArrayReuse792 = ((List) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3));
            }
            if (testEnumUnionArrayReuse792 != (null)) {
                testEnumUnionArrayReuse792 .clear();
                testEnumUnionArray790 = testEnumUnionArrayReuse792;
            } else {
                testEnumUnionArray790 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen791), testEnumUnionArray789);
            }
            do {
                for (int counter793 = 0; (counter793 <chunkLen791); counter793 ++) {
                    Object testEnumUnionArrayArrayElementReuseVar795 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3) instanceof GenericArray) {
                        testEnumUnionArrayArrayElementReuseVar795 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3)).peek();
                    }
                    int unionIndex796 = (decoder.readIndex());
                    if (unionIndex796 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex796 == 1) {
                        testEnumUnionArray790 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum780 .getEnumSymbols().get((decoder.readEnum()))));
                    }
                }
                chunkLen791 = (decoder.arrayNext());
            } while (chunkLen791 > 0);
        } else {
            testEnumUnionArray790 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumUnionArray789);
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(3, testEnumUnionArray790);
        return FastGenericDeserializerGeneratorTest_shouldReadEnum;
    }

}
