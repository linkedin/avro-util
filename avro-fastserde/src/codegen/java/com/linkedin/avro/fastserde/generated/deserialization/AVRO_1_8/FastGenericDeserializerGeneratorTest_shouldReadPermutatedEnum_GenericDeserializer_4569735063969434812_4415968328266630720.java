
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum_GenericDeserializer_4569735063969434812_4415968328266630720
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum755;
    private final Schema testEnumUnion758;
    private final Schema testEnumArray762;
    private final Schema testEnumUnionArray770;
    private final Schema testEnumUnionArrayArrayElemSchema775;

    public FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum_GenericDeserializer_4569735063969434812_4415968328266630720(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum755 = readerSchema.getField("testEnum").schema();
        this.testEnumUnion758 = readerSchema.getField("testEnumUnion").schema();
        this.testEnumArray762 = readerSchema.getField("testEnumArray").schema();
        this.testEnumUnionArray770 = readerSchema.getField("testEnumUnionArray").schema();
        this.testEnumUnionArrayArrayElemSchema775 = testEnumUnionArray770 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum754((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum754(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int enumIndex756 = (decoder.readEnum());
        org.apache.avro.generic.GenericData.EnumSymbol enumValue757 = null;
        if (enumIndex756 == 0) {
            enumValue757 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(1));
        }
        if (enumIndex756 == 1) {
            enumValue757 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(0));
        }
        if (enumIndex756 == 2) {
            enumValue757 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(4));
        }
        if (enumIndex756 == 3) {
            enumValue757 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(2));
        }
        if (enumIndex756 == 4) {
            enumValue757 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(3));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(0, enumValue757);
        int unionIndex759 = (decoder.readIndex());
        if (unionIndex759 == 0) {
            decoder.readNull();
        }
        if (unionIndex759 == 1) {
            int enumIndex760 = (decoder.readEnum());
            org.apache.avro.generic.GenericData.EnumSymbol enumValue761 = null;
            if (enumIndex760 == 0) {
                enumValue761 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(1));
            }
            if (enumIndex760 == 1) {
                enumValue761 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(0));
            }
            if (enumIndex760 == 2) {
                enumValue761 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(4));
            }
            if (enumIndex760 == 3) {
                enumValue761 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(2));
            }
            if (enumIndex760 == 4) {
                enumValue761 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(3));
            }
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(1, enumValue761);
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray763 = null;
        long chunkLen764 = (decoder.readArrayStart());
        if (chunkLen764 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArrayReuse765 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2) instanceof List) {
                testEnumArrayReuse765 = ((List) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2));
            }
            if (testEnumArrayReuse765 != (null)) {
                testEnumArrayReuse765 .clear();
                testEnumArray763 = testEnumArrayReuse765;
            } else {
                testEnumArray763 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen764), testEnumArray762);
            }
            do {
                for (int counter766 = 0; (counter766 <chunkLen764); counter766 ++) {
                    Object testEnumArrayArrayElementReuseVar767 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2) instanceof GenericArray) {
                        testEnumArrayArrayElementReuseVar767 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2)).peek();
                    }
                    int enumIndex768 = (decoder.readEnum());
                    org.apache.avro.generic.GenericData.EnumSymbol enumValue769 = null;
                    if (enumIndex768 == 0) {
                        enumValue769 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(1));
                    }
                    if (enumIndex768 == 1) {
                        enumValue769 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(0));
                    }
                    if (enumIndex768 == 2) {
                        enumValue769 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(4));
                    }
                    if (enumIndex768 == 3) {
                        enumValue769 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(2));
                    }
                    if (enumIndex768 == 4) {
                        enumValue769 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(3));
                    }
                    testEnumArray763 .add(enumValue769);
                }
                chunkLen764 = (decoder.arrayNext());
            } while (chunkLen764 > 0);
        } else {
            testEnumArray763 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumArray762);
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(2, testEnumArray763);
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray771 = null;
        long chunkLen772 = (decoder.readArrayStart());
        if (chunkLen772 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArrayReuse773 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3) instanceof List) {
                testEnumUnionArrayReuse773 = ((List) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3));
            }
            if (testEnumUnionArrayReuse773 != (null)) {
                testEnumUnionArrayReuse773 .clear();
                testEnumUnionArray771 = testEnumUnionArrayReuse773;
            } else {
                testEnumUnionArray771 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen772), testEnumUnionArray770);
            }
            do {
                for (int counter774 = 0; (counter774 <chunkLen772); counter774 ++) {
                    Object testEnumUnionArrayArrayElementReuseVar776 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3) instanceof GenericArray) {
                        testEnumUnionArrayArrayElementReuseVar776 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3)).peek();
                    }
                    int unionIndex777 = (decoder.readIndex());
                    if (unionIndex777 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex777 == 1) {
                        int enumIndex778 = (decoder.readEnum());
                        org.apache.avro.generic.GenericData.EnumSymbol enumValue779 = null;
                        if (enumIndex778 == 0) {
                            enumValue779 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(1));
                        }
                        if (enumIndex778 == 1) {
                            enumValue779 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(0));
                        }
                        if (enumIndex778 == 2) {
                            enumValue779 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(4));
                        }
                        if (enumIndex778 == 3) {
                            enumValue779 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(2));
                        }
                        if (enumIndex778 == 4) {
                            enumValue779 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum755, testEnum755 .getEnumSymbols().get(3));
                        }
                        testEnumUnionArray771 .add(enumValue779);
                    }
                }
                chunkLen772 = (decoder.arrayNext());
            } while (chunkLen772 > 0);
        } else {
            testEnumUnionArray771 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumUnionArray770);
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(3, testEnumUnionArray771);
        return FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum;
    }

}
