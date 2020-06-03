
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadEnum_GenericDeserializer_5976145119973076881_5976145119973076881
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum634;
    private final Schema testEnumUnion635;
    private final Schema testEnumArray637;
    private final Schema testEnumUnionArray643;
    private final Schema testEnumUnionArrayArrayElemSchema648;

    public FastGenericDeserializerGeneratorTest_shouldReadEnum_GenericDeserializer_5976145119973076881_5976145119973076881(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum634 = readerSchema.getField("testEnum").schema();
        this.testEnumUnion635 = readerSchema.getField("testEnumUnion").schema();
        this.testEnumArray637 = readerSchema.getField("testEnumArray").schema();
        this.testEnumUnionArray643 = readerSchema.getField("testEnumUnionArray").schema();
        this.testEnumUnionArrayArrayElemSchema648 = testEnumUnionArray643 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum633((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadEnum633(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadEnum;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadEnum = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadEnum = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(0, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum634, testEnum634 .getEnumSymbols().get((decoder.readEnum()))));
        int unionIndex636 = (decoder.readIndex());
        if (unionIndex636 == 0) {
            decoder.readNull();
        }
        if (unionIndex636 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadEnum.put(1, new org.apache.avro.generic.GenericData.EnumSymbol(testEnum634, testEnum634 .getEnumSymbols().get((decoder.readEnum()))));
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray638 = null;
        long chunkLen639 = (decoder.readArrayStart());
        if (chunkLen639 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArrayReuse640 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2) instanceof List) {
                testEnumArrayReuse640 = ((List) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2));
            }
            if (testEnumArrayReuse640 != (null)) {
                testEnumArrayReuse640 .clear();
                testEnumArray638 = testEnumArrayReuse640;
            } else {
                testEnumArray638 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen639), testEnumArray637);
            }
            do {
                for (int counter641 = 0; (counter641 <chunkLen639); counter641 ++) {
                    Object testEnumArrayArrayElementReuseVar642 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2) instanceof GenericArray) {
                        testEnumArrayArrayElementReuseVar642 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(2)).peek();
                    }
                    testEnumArray638 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum634, testEnum634 .getEnumSymbols().get((decoder.readEnum()))));
                }
                chunkLen639 = (decoder.arrayNext());
            } while (chunkLen639 > 0);
        } else {
            testEnumArray638 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumArray637);
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(2, testEnumArray638);
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray644 = null;
        long chunkLen645 = (decoder.readArrayStart());
        if (chunkLen645 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArrayReuse646 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3) instanceof List) {
                testEnumUnionArrayReuse646 = ((List) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3));
            }
            if (testEnumUnionArrayReuse646 != (null)) {
                testEnumUnionArrayReuse646 .clear();
                testEnumUnionArray644 = testEnumUnionArrayReuse646;
            } else {
                testEnumUnionArray644 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen645), testEnumUnionArray643);
            }
            do {
                for (int counter647 = 0; (counter647 <chunkLen645); counter647 ++) {
                    Object testEnumUnionArrayArrayElementReuseVar649 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3) instanceof GenericArray) {
                        testEnumUnionArrayArrayElementReuseVar649 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadEnum.get(3)).peek();
                    }
                    int unionIndex650 = (decoder.readIndex());
                    if (unionIndex650 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex650 == 1) {
                        testEnumUnionArray644 .add(new org.apache.avro.generic.GenericData.EnumSymbol(testEnum634, testEnum634 .getEnumSymbols().get((decoder.readEnum()))));
                    }
                }
                chunkLen645 = (decoder.arrayNext());
            } while (chunkLen645 > 0);
        } else {
            testEnumUnionArray644 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumUnionArray643);
        }
        FastGenericDeserializerGeneratorTest_shouldReadEnum.put(3, testEnumUnionArray644);
        return FastGenericDeserializerGeneratorTest_shouldReadEnum;
    }

}
