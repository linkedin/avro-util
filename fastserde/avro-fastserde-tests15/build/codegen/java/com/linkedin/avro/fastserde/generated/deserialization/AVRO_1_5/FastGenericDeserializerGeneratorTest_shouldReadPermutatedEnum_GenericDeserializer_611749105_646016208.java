
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_5;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum_GenericDeserializer_611749105_646016208
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema testEnum0;
    private final Map enumMappingtestEnum0;
    private final Schema testEnumUnion0;
    private final Map enumMappingtestEnum1;
    private final Schema testEnumArray0;
    private final Map enumMappingtestEnum2;
    private final Schema testEnumUnionArray0;
    private final Schema testEnumUnionArrayArrayElemSchema0;
    private final Map enumMappingtestEnum3;

    public FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum_GenericDeserializer_611749105_646016208(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.testEnum0 = readerSchema.getField("testEnum").schema();
        HashMap tempEnumMapping0 = new HashMap(5);
        tempEnumMapping0 .put(new Integer(0), new Integer(1));
        tempEnumMapping0 .put(new Integer(1), new Integer(0));
        tempEnumMapping0 .put(new Integer(2), new Integer(4));
        tempEnumMapping0 .put(new Integer(3), new Integer(2));
        tempEnumMapping0 .put(new Integer(4), new Integer(3));
        this.enumMappingtestEnum0 = Collections.unmodifiableMap(tempEnumMapping0);
        this.testEnumUnion0 = readerSchema.getField("testEnumUnion").schema();
        HashMap tempEnumMapping1 = new HashMap(5);
        tempEnumMapping1 .put(new Integer(0), new Integer(1));
        tempEnumMapping1 .put(new Integer(1), new Integer(0));
        tempEnumMapping1 .put(new Integer(2), new Integer(4));
        tempEnumMapping1 .put(new Integer(3), new Integer(2));
        tempEnumMapping1 .put(new Integer(4), new Integer(3));
        this.enumMappingtestEnum1 = Collections.unmodifiableMap(tempEnumMapping1);
        this.testEnumArray0 = readerSchema.getField("testEnumArray").schema();
        HashMap tempEnumMapping2 = new HashMap(5);
        tempEnumMapping2 .put(new Integer(0), new Integer(1));
        tempEnumMapping2 .put(new Integer(1), new Integer(0));
        tempEnumMapping2 .put(new Integer(2), new Integer(4));
        tempEnumMapping2 .put(new Integer(3), new Integer(2));
        tempEnumMapping2 .put(new Integer(4), new Integer(3));
        this.enumMappingtestEnum2 = Collections.unmodifiableMap(tempEnumMapping2);
        this.testEnumUnionArray0 = readerSchema.getField("testEnumUnionArray").schema();
        this.testEnumUnionArrayArrayElemSchema0 = testEnumUnionArray0 .getElementType();
        HashMap tempEnumMapping3 = new HashMap(5);
        tempEnumMapping3 .put(new Integer(0), new Integer(1));
        tempEnumMapping3 .put(new Integer(1), new Integer(0));
        tempEnumMapping3 .put(new Integer(2), new Integer(4));
        tempEnumMapping3 .put(new Integer(3), new Integer(2));
        tempEnumMapping3 .put(new Integer(4), new Integer(3));
        this.enumMappingtestEnum3 = Collections.unmodifiableMap(tempEnumMapping3);
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
        GenericEnumSymbol enumValue0 = null;
        Object enumIndexLookupResult0 = enumMappingtestEnum0 .get(enumIndex0);
        if (enumIndexLookupResult0 instanceof Integer) {
            enumValue0 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(((Integer) enumIndexLookupResult0)));
        } else {
            if (enumIndexLookupResult0 instanceof AvroTypeException) {
                throw((AvroTypeException) enumIndexLookupResult0);
            } else {
                throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.testEnum': "+ enumIndex0));
            }
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(0, enumValue0);
        populate_FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum0((FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum1((FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum), (decoder));
        return FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum0(IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(1, null);
        } else {
            if (unionIndex0 == 1) {
                int enumIndex1 = (decoder.readEnum());
                GenericEnumSymbol enumValue1 = null;
                Object enumIndexLookupResult1 = enumMappingtestEnum1 .get(enumIndex1);
                if (enumIndexLookupResult1 instanceof Integer) {
                    enumValue1 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(((Integer) enumIndexLookupResult1)));
                } else {
                    if (enumIndexLookupResult1 instanceof AvroTypeException) {
                        throw((AvroTypeException) enumIndexLookupResult1);
                    } else {
                        throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.testEnum': "+ enumIndex1));
                    }
                }
                FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(1, enumValue1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testEnumUnion': "+ unionIndex0));
            }
        }
        List<GenericEnumSymbol> testEnumArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(2);
        if (oldArray0 instanceof List) {
            testEnumArray1 = ((List) oldArray0);
            testEnumArray1 .clear();
        } else {
            testEnumArray1 = new org.apache.avro.generic.GenericData.Array<GenericEnumSymbol>(((int) chunkLen0), testEnumArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                int enumIndex2 = (decoder.readEnum());
                GenericEnumSymbol enumValue2 = null;
                Object enumIndexLookupResult2 = enumMappingtestEnum2 .get(enumIndex2);
                if (enumIndexLookupResult2 instanceof Integer) {
                    enumValue2 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(((Integer) enumIndexLookupResult2)));
                } else {
                    if (enumIndexLookupResult2 instanceof AvroTypeException) {
                        throw((AvroTypeException) enumIndexLookupResult2);
                    } else {
                        throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.testEnum': "+ enumIndex2));
                    }
                }
                testEnumArray1 .add(enumValue2);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(2, testEnumArray1);
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum1(IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum, Decoder decoder)
        throws IOException
    {
        List<GenericEnumSymbol> testEnumUnionArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray1 = FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.get(3);
        if (oldArray1 instanceof List) {
            testEnumUnionArray1 = ((List) oldArray1);
            testEnumUnionArray1 .clear();
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
                        int enumIndex3 = (decoder.readEnum());
                        GenericEnumSymbol enumValue3 = null;
                        Object enumIndexLookupResult3 = enumMappingtestEnum3 .get(enumIndex3);
                        if (enumIndexLookupResult3 instanceof Integer) {
                            enumValue3 = new org.apache.avro.generic.GenericData.EnumSymbol(testEnum0, testEnum0 .getEnumSymbols().get(((Integer) enumIndexLookupResult3)));
                        } else {
                            if (enumIndexLookupResult3 instanceof AvroTypeException) {
                                throw((AvroTypeException) enumIndexLookupResult3);
                            } else {
                                throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.testEnum': "+ enumIndex3));
                            }
                        }
                        testEnumUnionArray1 .add(enumValue3);
                    } else {
                        throw new RuntimeException(("Illegal union index for 'testEnumUnionArrayElem': "+ unionIndex1));
                    }
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadPermutatedEnum.put(3, testEnumUnionArray1);
    }

}
