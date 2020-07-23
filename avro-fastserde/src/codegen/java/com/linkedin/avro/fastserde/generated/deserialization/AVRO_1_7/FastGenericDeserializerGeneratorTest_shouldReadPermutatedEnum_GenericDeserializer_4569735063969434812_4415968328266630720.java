
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

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
    private final Schema testEnum0;
    private final Schema testEnumUnion0;
    private final Schema testEnumArray0;
    private final Schema testEnumUnionArray0;
    private final Schema testEnumUnionArrayArrayElemSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum_GenericDeserializer_4569735063969434812_4415968328266630720(Schema readerSchema) {
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
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int enumIndex0 = (decoder.readEnum());
        org.apache.avro.generic.GenericData.EnumSymbol enumValue0 = null;
        if (enumIndex0 == 0) {
            enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(1));
        } else {
            if (enumIndex0 == 1) {
                enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(0));
            } else {
                if (enumIndex0 == 2) {
                    enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(4));
                } else {
                    if (enumIndex0 == 3) {
                        enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(2));
                    } else {
                        if (enumIndex0 == 4) {
                            enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(3));
                        }
                    }
                }
            }
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(0, enumValue0);
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                int enumIndex1 = (decoder.readEnum());
                org.apache.avro.generic.GenericData.EnumSymbol enumValue1 = null;
                if (enumIndex1 == 0) {
                    enumValue1 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(1));
                } else {
                    if (enumIndex1 == 1) {
                        enumValue1 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(0));
                    } else {
                        if (enumIndex1 == 2) {
                            enumValue1 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(4));
                        } else {
                            if (enumIndex1 == 3) {
                                enumValue1 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(2));
                            } else {
                                if (enumIndex1 == 4) {
                                    enumValue1 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(3));
                                }
                            }
                        }
                    }
                }
                FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(1, enumValue1);
            }
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2) instanceof List) {
            testEnumArray1 = ((List) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2));
            testEnumArray1 .clear();
        } else {
            testEnumArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen0), testEnumArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                int enumIndex2 = (decoder.readEnum());
                org.apache.avro.generic.GenericData.EnumSymbol enumValue2 = null;
                if (enumIndex2 == 0) {
                    enumValue2 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(1));
                } else {
                    if (enumIndex2 == 1) {
                        enumValue2 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(0));
                    } else {
                        if (enumIndex2 == 2) {
                            enumValue2 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(4));
                        } else {
                            if (enumIndex2 == 3) {
                                enumValue2 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(2));
                            } else {
                                if (enumIndex2 == 4) {
                                    enumValue2 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(3));
                                }
                            }
                        }
                    }
                }
                testEnumArray1 .add(enumValue2);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(2, testEnumArray1);
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3) instanceof List) {
            testEnumUnionArray1 = ((List) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3));
            testEnumUnionArray1 .clear();
        } else {
            testEnumUnionArray1 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen1), testEnumUnionArray0);
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testEnumUnionArrayArrayElementReuseVar0 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3) instanceof GenericArray) {
                    testEnumUnionArrayArrayElementReuseVar0 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3)).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                if (unionIndex1 == 0) {
                    decoder.readNull();
                } else {
                    if (unionIndex1 == 1) {
                        int enumIndex3 = (decoder.readEnum());
                        org.apache.avro.generic.GenericData.EnumSymbol enumValue3 = null;
                        if (enumIndex3 == 0) {
                            enumValue3 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(1));
                        } else {
                            if (enumIndex3 == 1) {
                                enumValue3 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(0));
                            } else {
                                if (enumIndex3 == 2) {
                                    enumValue3 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(4));
                                } else {
                                    if (enumIndex3 == 3) {
                                        enumValue3 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(2));
                                    } else {
                                        if (enumIndex3 == 4) {
                                            enumValue3 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(3));
                                        }
                                    }
                                }
                            }
                        }
                        testEnumUnionArray1 .add(enumValue3);
                    }
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(3, testEnumUnionArray1);
        return FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum;
    }

}
