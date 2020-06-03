
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum_GenericDeserializer_405768074940221895_541502821296456517
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum901;
    private final Schema testEnumUnion904;
    private final Schema testEnumArray908;
    private final Schema testEnumUnionArray916;
    private final Schema testEnumUnionArrayArrayElemSchema921;

    public FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum_GenericDeserializer_405768074940221895_541502821296456517(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum901 = readerSchema.getField("testEnum").schema();
        this.testEnumUnion904 = readerSchema.getField("testEnumUnion").schema();
        this.testEnumArray908 = readerSchema.getField("testEnumArray").schema();
        this.testEnumUnionArray916 = readerSchema.getField("testEnumUnionArray").schema();
        this.testEnumUnionArrayArrayElemSchema921 = testEnumUnionArray916 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum900((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum900(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int enumIndex902 = (decoder.readEnum());
        org.apache.avro.generic.GenericData.EnumSymbol enumValue903 = null;
        if (enumIndex902 == 0) {
            enumValue903 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(1));
        }
        if (enumIndex902 == 1) {
            enumValue903 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(0));
        }
        if (enumIndex902 == 2) {
            enumValue903 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(4));
        }
        if (enumIndex902 == 3) {
            enumValue903 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(2));
        }
        if (enumIndex902 == 4) {
            enumValue903 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(3));
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(0, enumValue903);
        int unionIndex905 = (decoder.readIndex());
        if (unionIndex905 == 0) {
            decoder.readNull();
        }
        if (unionIndex905 == 1) {
            int enumIndex906 = (decoder.readEnum());
            org.apache.avro.generic.GenericData.EnumSymbol enumValue907 = null;
            if (enumIndex906 == 0) {
                enumValue907 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(1));
            }
            if (enumIndex906 == 1) {
                enumValue907 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(0));
            }
            if (enumIndex906 == 2) {
                enumValue907 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(4));
            }
            if (enumIndex906 == 3) {
                enumValue907 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(2));
            }
            if (enumIndex906 == 4) {
                enumValue907 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(3));
            }
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(1, enumValue907);
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray909 = null;
        long chunkLen910 = (decoder.readArrayStart());
        if (chunkLen910 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArrayReuse911 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2) instanceof List) {
                testEnumArrayReuse911 = ((List) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2));
            }
            if (testEnumArrayReuse911 != (null)) {
                testEnumArrayReuse911 .clear();
                testEnumArray909 = testEnumArrayReuse911;
            } else {
                testEnumArray909 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen910), testEnumArray908);
            }
            do {
                for (int counter912 = 0; (counter912 <chunkLen910); counter912 ++) {
                    Object testEnumArrayArrayElementReuseVar913 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2) instanceof GenericArray) {
                        testEnumArrayArrayElementReuseVar913 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2)).peek();
                    }
                    int enumIndex914 = (decoder.readEnum());
                    org.apache.avro.generic.GenericData.EnumSymbol enumValue915 = null;
                    if (enumIndex914 == 0) {
                        enumValue915 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(1));
                    }
                    if (enumIndex914 == 1) {
                        enumValue915 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(0));
                    }
                    if (enumIndex914 == 2) {
                        enumValue915 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(4));
                    }
                    if (enumIndex914 == 3) {
                        enumValue915 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(2));
                    }
                    if (enumIndex914 == 4) {
                        enumValue915 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(3));
                    }
                    testEnumArray909 .add(enumValue915);
                }
                chunkLen910 = (decoder.arrayNext());
            } while (chunkLen910 > 0);
        } else {
            testEnumArray909 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumArray908);
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(2, testEnumArray909);
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray917 = null;
        long chunkLen918 = (decoder.readArrayStart());
        if (chunkLen918 > 0) {
            List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArrayReuse919 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3) instanceof List) {
                testEnumUnionArrayReuse919 = ((List) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3));
            }
            if (testEnumUnionArrayReuse919 != (null)) {
                testEnumUnionArrayReuse919 .clear();
                testEnumUnionArray917 = testEnumUnionArrayReuse919;
            } else {
                testEnumUnionArray917 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(((int) chunkLen918), testEnumUnionArray916);
            }
            do {
                for (int counter920 = 0; (counter920 <chunkLen918); counter920 ++) {
                    Object testEnumUnionArrayArrayElementReuseVar922 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3) instanceof GenericArray) {
                        testEnumUnionArrayArrayElementReuseVar922 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3)).peek();
                    }
                    int unionIndex923 = (decoder.readIndex());
                    if (unionIndex923 == 0) {
                        decoder.readNull();
                    }
                    if (unionIndex923 == 1) {
                        int enumIndex924 = (decoder.readEnum());
                        org.apache.avro.generic.GenericData.EnumSymbol enumValue925 = null;
                        if (enumIndex924 == 0) {
                            enumValue925 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(1));
                        }
                        if (enumIndex924 == 1) {
                            enumValue925 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(0));
                        }
                        if (enumIndex924 == 2) {
                            enumValue925 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(4));
                        }
                        if (enumIndex924 == 3) {
                            enumValue925 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(2));
                        }
                        if (enumIndex924 == 4) {
                            enumValue925 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum901 .getEnumSymbols().get(3));
                        }
                        testEnumUnionArray917 .add(enumValue925);
                    }
                }
                chunkLen918 = (decoder.arrayNext());
            } while (chunkLen918 > 0);
        } else {
            testEnumUnionArray917 = new org.apache.avro.generic.GenericData.Array<org.apache.avro.generic.GenericData.EnumSymbol>(0, testEnumUnionArray916);
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(3, testEnumUnionArray917);
        return FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum;
    }

}
