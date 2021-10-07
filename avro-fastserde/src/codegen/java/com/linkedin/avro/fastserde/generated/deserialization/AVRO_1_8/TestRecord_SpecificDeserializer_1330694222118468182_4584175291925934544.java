
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class TestRecord_SpecificDeserializer_1330694222118468182_4584175291925934544
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.TestRecord>
{

    private final Schema readerSchema;

    public TestRecord_SpecificDeserializer_1330694222118468182_4584175291925934544(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.TestRecord deserialize(com.linkedin.avro.fastserde.generated.avro.TestRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeTestRecord0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.TestRecord deserializeTestRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord;
        if ((reuse)!= null) {
            TestRecord = ((com.linkedin.avro.fastserde.generated.avro.TestRecord)(reuse));
        } else {
            TestRecord = new com.linkedin.avro.fastserde.generated.avro.TestRecord();
        }
        TestRecord.put(0, (decoder.readInt()));
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                TestRecord.put(1, (decoder.readInt()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testIntUnion': "+ unionIndex0));
        }
        TestRecord.put(2, (decoder.readLong()));
        int unionIndex1 = (decoder.readIndex());
        switch (unionIndex1) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                TestRecord.put(3, (decoder.readLong()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testLongUnion': "+ unionIndex1));
        }
        TestRecord.put(4, (decoder.readDouble()));
        int unionIndex2 = (decoder.readIndex());
        switch (unionIndex2) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                TestRecord.put(5, (decoder.readDouble()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testDoubleUnion': "+ unionIndex2));
        }
        TestRecord.put(6, (decoder.readFloat()));
        int unionIndex3 = (decoder.readIndex());
        switch (unionIndex3) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                TestRecord.put(7, (decoder.readFloat()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testFloatUnion': "+ unionIndex3));
        }
        TestRecord.put(8, (decoder.readBoolean()));
        int unionIndex4 = (decoder.readIndex());
        switch (unionIndex4) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                TestRecord.put(9, (decoder.readBoolean()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'testBooleanUnion': "+ unionIndex4));
        }
        Object oldBytes0 = TestRecord.get(10);
        if (oldBytes0 instanceof ByteBuffer) {
            TestRecord.put(10, (decoder).readBytes(((ByteBuffer) oldBytes0)));
        } else {
            TestRecord.put(10, (decoder).readBytes((null)));
        }
        int unionIndex5 = (decoder.readIndex());
        switch (unionIndex5) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldBytes1 = TestRecord.get(11);
                if (oldBytes1 instanceof ByteBuffer) {
                    TestRecord.put(11, (decoder).readBytes(((ByteBuffer) oldBytes1)));
                } else {
                    TestRecord.put(11, (decoder).readBytes((null)));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testBytesUnion': "+ unionIndex5));
        }
        Object oldString0 = TestRecord.get(12);
        if (oldString0 instanceof Utf8) {
            TestRecord.put(12, (decoder).readString(((Utf8) oldString0)));
        } else {
            TestRecord.put(12, (decoder).readString(null));
        }
        int unionIndex6 = (decoder.readIndex());
        switch (unionIndex6) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString1 = TestRecord.get(13);
                if (oldString1 instanceof Utf8) {
                    TestRecord.put(13, (decoder).readString(((Utf8) oldString1)));
                } else {
                    TestRecord.put(13, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testStringUnion': "+ unionIndex6));
        }
        int unionIndex7 = (decoder.readIndex());
        switch (unionIndex7) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                decoder.skipString();
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'removedField': "+ unionIndex7));
        }
        byte[] testFixed0;
        Object oldFixed0 = TestRecord.get(14);
        if ((oldFixed0 instanceof GenericFixed)&&(((GenericFixed) oldFixed0).bytes().length == (1))) {
            testFixed0 = ((GenericFixed) oldFixed0).bytes();
        } else {
            testFixed0 = ( new byte[1]);
        }
        decoder.readFixed(testFixed0);
        TestFixed testFixed1;
        testFixed1 = new TestFixed();
        testFixed1.bytes(testFixed0);
        TestRecord.put(14, testFixed1);
        int unionIndex8 = (decoder.readIndex());
        switch (unionIndex8) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                byte[] testFixed2;
                Object oldFixed1 = TestRecord.get(15);
                if ((oldFixed1 instanceof GenericFixed)&&(((GenericFixed) oldFixed1).bytes().length == (1))) {
                    testFixed2 = ((GenericFixed) oldFixed1).bytes();
                } else {
                    testFixed2 = ( new byte[1]);
                }
                decoder.readFixed(testFixed2);
                TestFixed testFixed3;
                testFixed3 = new TestFixed();
                testFixed3.bytes(testFixed2);
                TestRecord.put(15, testFixed3);
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testFixedUnion': "+ unionIndex8));
        }
        List<TestFixed> testFixedArray0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = TestRecord.get(16);
        if (oldArray0 instanceof List) {
            testFixedArray0 = ((List) oldArray0);
            testFixedArray0 .clear();
        } else {
            testFixedArray0 = new ArrayList<TestFixed>(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object testFixedArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    testFixedArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                byte[] testFixed4;
                Object oldFixed2 = testFixedArrayArrayElementReuseVar0;
                if ((oldFixed2 instanceof GenericFixed)&&(((GenericFixed) oldFixed2).bytes().length == (1))) {
                    testFixed4 = ((GenericFixed) oldFixed2).bytes();
                } else {
                    testFixed4 = ( new byte[1]);
                }
                decoder.readFixed(testFixed4);
                TestFixed testFixed5;
                testFixed5 = new TestFixed();
                testFixed5.bytes(testFixed4);
                testFixedArray0 .add(testFixed5);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        TestRecord.put(16, testFixedArray0);
        List<TestFixed> testFixedUnionArray0 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray1 = TestRecord.get(17);
        if (oldArray1 instanceof List) {
            testFixedUnionArray0 = ((List) oldArray1);
            testFixedUnionArray0 .clear();
        } else {
            testFixedUnionArray0 = new ArrayList<TestFixed>(((int) chunkLen1));
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testFixedUnionArrayArrayElementReuseVar0 = null;
                if (oldArray1 instanceof GenericArray) {
                    testFixedUnionArrayArrayElementReuseVar0 = ((GenericArray) oldArray1).peek();
                }
                int unionIndex9 = (decoder.readIndex());
                switch (unionIndex9) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                    {
                        byte[] testFixed6;
                        Object oldFixed3 = testFixedUnionArrayArrayElementReuseVar0;
                        if ((oldFixed3 instanceof GenericFixed)&&(((GenericFixed) oldFixed3).bytes().length == (1))) {
                            testFixed6 = ((GenericFixed) oldFixed3).bytes();
                        } else {
                            testFixed6 = ( new byte[1]);
                        }
                        decoder.readFixed(testFixed6);
                        TestFixed testFixed7;
                        testFixed7 = new TestFixed();
                        testFixed7.bytes(testFixed6);
                        testFixedUnionArray0 .add(testFixed7);
                        break;
                    }
                    default:
                        throw new RuntimeException(("Illegal union index for 'testFixedUnionArrayElem': "+ unionIndex9));
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        TestRecord.put(17, testFixedUnionArray0);
        int enumIndex0 = (decoder.readEnum());
        TestEnum enumValue0 = null;
        switch (enumIndex0) {
            case  0 :
                enumValue0 = TestEnum.values()[ 4 ];
                break;
            case  1 :
                enumValue0 = TestEnum.values()[ 3 ];
                break;
            case  2 :
                enumValue0 = TestEnum.values()[ 1 ];
                break;
            case  3 :
                enumValue0 = TestEnum.values()[ 2 ];
                break;
            case  4 :
                enumValue0 = TestEnum.values()[ 0 ];
                break;
            case  5 :
                throw new AvroTypeException("com.linkedin.avro.fastserde.generated.avro.TestEnum: No match for F");
            default:
                throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.TestEnum': "+ enumIndex0));
        }
        TestRecord.put(18, enumValue0);
        int unionIndex10 = (decoder.readIndex());
        switch (unionIndex10) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                int enumIndex1 = (decoder.readEnum());
                TestEnum enumValue1 = null;
                switch (enumIndex1) {
                    case  0 :
                        enumValue1 = TestEnum.values()[ 4 ];
                        break;
                    case  1 :
                        enumValue1 = TestEnum.values()[ 3 ];
                        break;
                    case  2 :
                        enumValue1 = TestEnum.values()[ 1 ];
                        break;
                    case  3 :
                        enumValue1 = TestEnum.values()[ 2 ];
                        break;
                    case  4 :
                        enumValue1 = TestEnum.values()[ 0 ];
                        break;
                    case  5 :
                        throw new AvroTypeException("com.linkedin.avro.fastserde.generated.avro.TestEnum: No match for F");
                    default:
                        throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.TestEnum': "+ enumIndex1));
                }
                TestRecord.put(19, enumValue1);
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'testEnumUnion': "+ unionIndex10));
        }
        List<TestEnum> testEnumArray0 = null;
        long chunkLen2 = (decoder.readArrayStart());
        Object oldArray2 = TestRecord.get(20);
        if (oldArray2 instanceof List) {
            testEnumArray0 = ((List) oldArray2);
            testEnumArray0 .clear();
        } else {
            testEnumArray0 = new ArrayList<TestEnum>(((int) chunkLen2));
        }
        while (chunkLen2 > 0) {
            for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                int enumIndex2 = (decoder.readEnum());
                TestEnum enumValue2 = null;
                switch (enumIndex2) {
                    case  0 :
                        enumValue2 = TestEnum.values()[ 4 ];
                        break;
                    case  1 :
                        enumValue2 = TestEnum.values()[ 3 ];
                        break;
                    case  2 :
                        enumValue2 = TestEnum.values()[ 1 ];
                        break;
                    case  3 :
                        enumValue2 = TestEnum.values()[ 2 ];
                        break;
                    case  4 :
                        enumValue2 = TestEnum.values()[ 0 ];
                        break;
                    case  5 :
                        throw new AvroTypeException("com.linkedin.avro.fastserde.generated.avro.TestEnum: No match for F");
                    default:
                        throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.TestEnum': "+ enumIndex2));
                }
                testEnumArray0 .add(enumValue2);
            }
            chunkLen2 = (decoder.arrayNext());
        }
        TestRecord.put(20, testEnumArray0);
        List<TestEnum> testEnumUnionArray0 = null;
        long chunkLen3 = (decoder.readArrayStart());
        Object oldArray3 = TestRecord.get(21);
        if (oldArray3 instanceof List) {
            testEnumUnionArray0 = ((List) oldArray3);
            testEnumUnionArray0 .clear();
        } else {
            testEnumUnionArray0 = new ArrayList<TestEnum>(((int) chunkLen3));
        }
        while (chunkLen3 > 0) {
            for (int counter3 = 0; (counter3 <chunkLen3); counter3 ++) {
                Object testEnumUnionArrayArrayElementReuseVar0 = null;
                if (oldArray3 instanceof GenericArray) {
                    testEnumUnionArrayArrayElementReuseVar0 = ((GenericArray) oldArray3).peek();
                }
                int unionIndex11 = (decoder.readIndex());
                switch (unionIndex11) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                    {
                        int enumIndex3 = (decoder.readEnum());
                        TestEnum enumValue3 = null;
                        switch (enumIndex3) {
                            case  0 :
                                enumValue3 = TestEnum.values()[ 4 ];
                                break;
                            case  1 :
                                enumValue3 = TestEnum.values()[ 3 ];
                                break;
                            case  2 :
                                enumValue3 = TestEnum.values()[ 1 ];
                                break;
                            case  3 :
                                enumValue3 = TestEnum.values()[ 2 ];
                                break;
                            case  4 :
                                enumValue3 = TestEnum.values()[ 0 ];
                                break;
                            case  5 :
                                throw new AvroTypeException("com.linkedin.avro.fastserde.generated.avro.TestEnum: No match for F");
                            default:
                                throw new RuntimeException(("Illegal enum index for 'com.linkedin.avro.fastserde.generated.avro.TestEnum': "+ enumIndex3));
                        }
                        testEnumUnionArray0 .add(enumValue3);
                        break;
                    }
                    default:
                        throw new RuntimeException(("Illegal union index for 'testEnumUnionArrayElem': "+ unionIndex11));
                }
            }
            chunkLen3 = (decoder.arrayNext());
        }
        TestRecord.put(21, testEnumUnionArray0);
        int unionIndex12 = (decoder.readIndex());
        switch (unionIndex12) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                TestRecord.put(22, deserializeSubRecord0(TestRecord.get(22), (decoder)));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'subRecordUnion': "+ unionIndex12));
        }
        TestRecord.put(23, deserializeSubRecord0(TestRecord.get(23), (decoder)));
        List<com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsArray0 = null;
        long chunkLen4 = (decoder.readArrayStart());
        Object oldArray4 = TestRecord.get(24);
        if (oldArray4 instanceof List) {
            recordsArray0 = ((List) oldArray4);
            recordsArray0 .clear();
        } else {
            recordsArray0 = new ArrayList<com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int) chunkLen4));
        }
        while (chunkLen4 > 0) {
            for (int counter4 = 0; (counter4 <chunkLen4); counter4 ++) {
                Object recordsArrayArrayElementReuseVar0 = null;
                if (oldArray4 instanceof GenericArray) {
                    recordsArrayArrayElementReuseVar0 = ((GenericArray) oldArray4).peek();
                }
                recordsArray0 .add(deserializeSubRecord0(recordsArrayArrayElementReuseVar0, (decoder)));
            }
            chunkLen4 = (decoder.arrayNext());
        }
        TestRecord.put(24, recordsArray0);
        Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsMap0 = null;
        long chunkLen5 = (decoder.readMapStart());
        if (chunkLen5 > 0) {
            Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsMapReuse0 = null;
            Object oldMap0 = TestRecord.get(25);
            if (oldMap0 instanceof Map) {
                recordsMapReuse0 = ((Map) oldMap0);
            }
            if (recordsMapReuse0 != (null)) {
                recordsMapReuse0 .clear();
                recordsMap0 = recordsMapReuse0;
            } else {
                recordsMap0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int)(((chunkLen5 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter5 = 0; (counter5 <chunkLen5); counter5 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    recordsMap0 .put(key0, deserializeSubRecord0(null, (decoder)));
                }
                chunkLen5 = (decoder.mapNext());
            } while (chunkLen5 > 0);
        } else {
            recordsMap0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(0);
        }
        TestRecord.put(25, recordsMap0);
        int unionIndex16 = (decoder.readIndex());
        switch (unionIndex16) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                List<com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsArrayUnionOption0 = null;
                long chunkLen6 = (decoder.readArrayStart());
                Object oldArray5 = TestRecord.get(26);
                if (oldArray5 instanceof List) {
                    recordsArrayUnionOption0 = ((List) oldArray5);
                    recordsArrayUnionOption0 .clear();
                } else {
                    recordsArrayUnionOption0 = new ArrayList<com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int) chunkLen6));
                }
                while (chunkLen6 > 0) {
                    for (int counter6 = 0; (counter6 <chunkLen6); counter6 ++) {
                        Object recordsArrayUnionOptionArrayElementReuseVar0 = null;
                        if (oldArray5 instanceof GenericArray) {
                            recordsArrayUnionOptionArrayElementReuseVar0 = ((GenericArray) oldArray5).peek();
                        }
                        int unionIndex17 = (decoder.readIndex());
                        switch (unionIndex17) {
                            case  0 :
                                decoder.readNull();
                                break;
                            case  1 :
                                recordsArrayUnionOption0 .add(deserializeSubRecord0(recordsArrayUnionOptionArrayElementReuseVar0, (decoder)));
                                break;
                            default:
                                throw new RuntimeException(("Illegal union index for 'recordsArrayUnionOptionElem': "+ unionIndex17));
                        }
                    }
                    chunkLen6 = (decoder.arrayNext());
                }
                TestRecord.put(26, recordsArrayUnionOption0);
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'recordsArrayUnion': "+ unionIndex16));
        }
        int unionIndex18 = (decoder.readIndex());
        switch (unionIndex18) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsMapUnionOption0 = null;
                long chunkLen7 = (decoder.readMapStart());
                if (chunkLen7 > 0) {
                    Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsMapUnionOptionReuse0 = null;
                    Object oldMap1 = TestRecord.get(27);
                    if (oldMap1 instanceof Map) {
                        recordsMapUnionOptionReuse0 = ((Map) oldMap1);
                    }
                    if (recordsMapUnionOptionReuse0 != (null)) {
                        recordsMapUnionOptionReuse0 .clear();
                        recordsMapUnionOption0 = recordsMapUnionOptionReuse0;
                    } else {
                        recordsMapUnionOption0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int)(((chunkLen7 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter7 = 0; (counter7 <chunkLen7); counter7 ++) {
                            Utf8 key1 = (decoder.readString(null));
                            int unionIndex19 = (decoder.readIndex());
                            switch (unionIndex19) {
                                case  0 :
                                    decoder.readNull();
                                    break;
                                case  1 :
                                    recordsMapUnionOption0 .put(key1, deserializeSubRecord0(null, (decoder)));
                                    break;
                                default:
                                    throw new RuntimeException(("Illegal union index for 'recordsMapUnionOptionValue': "+ unionIndex19));
                            }
                        }
                        chunkLen7 = (decoder.mapNext());
                    } while (chunkLen7 > 0);
                } else {
                    recordsMapUnionOption0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(0);
                }
                TestRecord.put(27, recordsMapUnionOption0);
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'recordsMapUnion': "+ unionIndex18));
        }
        List<Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>> recordsArrayMap0 = null;
        long chunkLen8 = (decoder.readArrayStart());
        Object oldArray6 = TestRecord.get(28);
        if (oldArray6 instanceof List) {
            recordsArrayMap0 = ((List) oldArray6);
            recordsArrayMap0 .clear();
        } else {
            recordsArrayMap0 = new ArrayList<Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>>(((int) chunkLen8));
        }
        while (chunkLen8 > 0) {
            for (int counter8 = 0; (counter8 <chunkLen8); counter8 ++) {
                Object recordsArrayMapArrayElementReuseVar0 = null;
                if (oldArray6 instanceof GenericArray) {
                    recordsArrayMapArrayElementReuseVar0 = ((GenericArray) oldArray6).peek();
                }
                Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsArrayMapElem0 = null;
                long chunkLen9 = (decoder.readMapStart());
                if (chunkLen9 > 0) {
                    Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsArrayMapElemReuse0 = null;
                    if (recordsArrayMapArrayElementReuseVar0 instanceof Map) {
                        recordsArrayMapElemReuse0 = ((Map) recordsArrayMapArrayElementReuseVar0);
                    }
                    if (recordsArrayMapElemReuse0 != (null)) {
                        recordsArrayMapElemReuse0 .clear();
                        recordsArrayMapElem0 = recordsArrayMapElemReuse0;
                    } else {
                        recordsArrayMapElem0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int)(((chunkLen9 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter9 = 0; (counter9 <chunkLen9); counter9 ++) {
                            Utf8 key2 = (decoder.readString(null));
                            int unionIndex20 = (decoder.readIndex());
                            switch (unionIndex20) {
                                case  0 :
                                    decoder.readNull();
                                    break;
                                case  1 :
                                    recordsArrayMapElem0 .put(key2, deserializeSubRecord0(null, (decoder)));
                                    break;
                                default:
                                    throw new RuntimeException(("Illegal union index for 'recordsArrayMapElemValue': "+ unionIndex20));
                            }
                        }
                        chunkLen9 = (decoder.mapNext());
                    } while (chunkLen9 > 0);
                } else {
                    recordsArrayMapElem0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(0);
                }
                recordsArrayMap0 .add(recordsArrayMapElem0);
            }
            chunkLen8 = (decoder.arrayNext());
        }
        TestRecord.put(28, recordsArrayMap0);
        Map<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>> recordsMapArray0 = null;
        long chunkLen10 = (decoder.readMapStart());
        if (chunkLen10 > 0) {
            Map<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>> recordsMapArrayReuse0 = null;
            Object oldMap2 = TestRecord.get(29);
            if (oldMap2 instanceof Map) {
                recordsMapArrayReuse0 = ((Map) oldMap2);
            }
            if (recordsMapArrayReuse0 != (null)) {
                recordsMapArrayReuse0 .clear();
                recordsMapArray0 = recordsMapArrayReuse0;
            } else {
                recordsMapArray0 = new HashMap<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>>(((int)(((chunkLen10 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter10 = 0; (counter10 <chunkLen10); counter10 ++) {
                    Utf8 key3 = (decoder.readString(null));
                    List<com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsMapArrayValue0 = null;
                    long chunkLen11 = (decoder.readArrayStart());
                    if (null instanceof List) {
                        recordsMapArrayValue0 = ((List) null);
                        recordsMapArrayValue0 .clear();
                    } else {
                        recordsMapArrayValue0 = new ArrayList<com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int) chunkLen11));
                    }
                    while (chunkLen11 > 0) {
                        for (int counter11 = 0; (counter11 <chunkLen11); counter11 ++) {
                            Object recordsMapArrayValueArrayElementReuseVar0 = null;
                            if (null instanceof GenericArray) {
                                recordsMapArrayValueArrayElementReuseVar0 = ((GenericArray) null).peek();
                            }
                            int unionIndex21 = (decoder.readIndex());
                            switch (unionIndex21) {
                                case  0 :
                                    decoder.readNull();
                                    break;
                                case  1 :
                                    recordsMapArrayValue0 .add(deserializeSubRecord0(recordsMapArrayValueArrayElementReuseVar0, (decoder)));
                                    break;
                                default:
                                    throw new RuntimeException(("Illegal union index for 'recordsMapArrayValueElem': "+ unionIndex21));
                            }
                        }
                        chunkLen11 = (decoder.arrayNext());
                    }
                    recordsMapArray0 .put(key3, recordsMapArrayValue0);
                }
                chunkLen10 = (decoder.mapNext());
            } while (chunkLen10 > 0);
        } else {
            recordsMapArray0 = new HashMap<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>>(0);
        }
        TestRecord.put(29, recordsMapArray0);
        int unionIndex22 = (decoder.readIndex());
        switch (unionIndex22) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                List<Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>> recordsArrayMapUnionOption0 = null;
                long chunkLen12 = (decoder.readArrayStart());
                Object oldArray7 = TestRecord.get(30);
                if (oldArray7 instanceof List) {
                    recordsArrayMapUnionOption0 = ((List) oldArray7);
                    recordsArrayMapUnionOption0 .clear();
                } else {
                    recordsArrayMapUnionOption0 = new ArrayList<Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>>(((int) chunkLen12));
                }
                while (chunkLen12 > 0) {
                    for (int counter12 = 0; (counter12 <chunkLen12); counter12 ++) {
                        Object recordsArrayMapUnionOptionArrayElementReuseVar0 = null;
                        if (oldArray7 instanceof GenericArray) {
                            recordsArrayMapUnionOptionArrayElementReuseVar0 = ((GenericArray) oldArray7).peek();
                        }
                        Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsArrayMapUnionOptionElem0 = null;
                        long chunkLen13 = (decoder.readMapStart());
                        if (chunkLen13 > 0) {
                            Map<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsArrayMapUnionOptionElemReuse0 = null;
                            if (recordsArrayMapUnionOptionArrayElementReuseVar0 instanceof Map) {
                                recordsArrayMapUnionOptionElemReuse0 = ((Map) recordsArrayMapUnionOptionArrayElementReuseVar0);
                            }
                            if (recordsArrayMapUnionOptionElemReuse0 != (null)) {
                                recordsArrayMapUnionOptionElemReuse0 .clear();
                                recordsArrayMapUnionOptionElem0 = recordsArrayMapUnionOptionElemReuse0;
                            } else {
                                recordsArrayMapUnionOptionElem0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int)(((chunkLen13 * 4)+ 2)/ 3)));
                            }
                            do {
                                for (int counter13 = 0; (counter13 <chunkLen13); counter13 ++) {
                                    Utf8 key4 = (decoder.readString(null));
                                    int unionIndex23 = (decoder.readIndex());
                                    switch (unionIndex23) {
                                        case  0 :
                                            decoder.readNull();
                                            break;
                                        case  1 :
                                            recordsArrayMapUnionOptionElem0 .put(key4, deserializeSubRecord0(null, (decoder)));
                                            break;
                                        default:
                                            throw new RuntimeException(("Illegal union index for 'recordsArrayMapUnionOptionElemValue': "+ unionIndex23));
                                    }
                                }
                                chunkLen13 = (decoder.mapNext());
                            } while (chunkLen13 > 0);
                        } else {
                            recordsArrayMapUnionOptionElem0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(0);
                        }
                        recordsArrayMapUnionOption0 .add(recordsArrayMapUnionOptionElem0);
                    }
                    chunkLen12 = (decoder.arrayNext());
                }
                TestRecord.put(30, recordsArrayMapUnionOption0);
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'recordsArrayMapUnion': "+ unionIndex22));
        }
        int unionIndex24 = (decoder.readIndex());
        switch (unionIndex24) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Map<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>> recordsMapArrayUnionOption0 = null;
                long chunkLen14 = (decoder.readMapStart());
                if (chunkLen14 > 0) {
                    Map<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>> recordsMapArrayUnionOptionReuse0 = null;
                    Object oldMap3 = TestRecord.get(31);
                    if (oldMap3 instanceof Map) {
                        recordsMapArrayUnionOptionReuse0 = ((Map) oldMap3);
                    }
                    if (recordsMapArrayUnionOptionReuse0 != (null)) {
                        recordsMapArrayUnionOptionReuse0 .clear();
                        recordsMapArrayUnionOption0 = recordsMapArrayUnionOptionReuse0;
                    } else {
                        recordsMapArrayUnionOption0 = new HashMap<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>>(((int)(((chunkLen14 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter14 = 0; (counter14 <chunkLen14); counter14 ++) {
                            Utf8 key5 = (decoder.readString(null));
                            List<com.linkedin.avro.fastserde.generated.avro.SubRecord> recordsMapArrayUnionOptionValue0 = null;
                            long chunkLen15 = (decoder.readArrayStart());
                            if (null instanceof List) {
                                recordsMapArrayUnionOptionValue0 = ((List) null);
                                recordsMapArrayUnionOptionValue0 .clear();
                            } else {
                                recordsMapArrayUnionOptionValue0 = new ArrayList<com.linkedin.avro.fastserde.generated.avro.SubRecord>(((int) chunkLen15));
                            }
                            while (chunkLen15 > 0) {
                                for (int counter15 = 0; (counter15 <chunkLen15); counter15 ++) {
                                    Object recordsMapArrayUnionOptionValueArrayElementReuseVar0 = null;
                                    if (null instanceof GenericArray) {
                                        recordsMapArrayUnionOptionValueArrayElementReuseVar0 = ((GenericArray) null).peek();
                                    }
                                    int unionIndex25 = (decoder.readIndex());
                                    switch (unionIndex25) {
                                        case  0 :
                                            decoder.readNull();
                                            break;
                                        case  1 :
                                            recordsMapArrayUnionOptionValue0 .add(deserializeSubRecord0(recordsMapArrayUnionOptionValueArrayElementReuseVar0, (decoder)));
                                            break;
                                        default:
                                            throw new RuntimeException(("Illegal union index for 'recordsMapArrayUnionOptionValueElem': "+ unionIndex25));
                                    }
                                }
                                chunkLen15 = (decoder.arrayNext());
                            }
                            recordsMapArrayUnionOption0 .put(key5, recordsMapArrayUnionOptionValue0);
                        }
                        chunkLen14 = (decoder.mapNext());
                    } while (chunkLen14 > 0);
                } else {
                    recordsMapArrayUnionOption0 = new HashMap<Utf8, List<com.linkedin.avro.fastserde.generated.avro.SubRecord>>(0);
                }
                TestRecord.put(31, recordsMapArrayUnionOption0);
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'recordsMapArrayUnion': "+ unionIndex24));
        }
        int unionIndex26 = (decoder.readIndex());
        switch (unionIndex26) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                TestRecord.put(32, deserializeSubRecord0(TestRecord.get(32), (decoder)));
                break;
            case  2 :
            {
                Object oldString4 = TestRecord.get(32);
                if (oldString4 instanceof Utf8) {
                    TestRecord.put(32, (decoder).readString(((Utf8) oldString4)));
                } else {
                    TestRecord.put(32, (decoder).readString(null));
                }
                break;
            }
            case  3 :
                TestRecord.put(32, (decoder.readInt()));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex26));
        }
        ArrayList<Boolean> defaultArray0 = new ArrayList<Boolean>();
        TestRecord.put(33, defaultArray0);
        ArrayList<Double> defaultArray1 = new ArrayList<Double>();
        TestRecord.put(34, defaultArray1);
        ArrayList<Float> defaultArray2 = new ArrayList<Float>();
        TestRecord.put(35, defaultArray2);
        ArrayList<Integer> defaultArray3 = new ArrayList<Integer>();
        TestRecord.put(36, defaultArray3);
        ArrayList<Long> defaultArray4 = new ArrayList<Long>();
        TestRecord.put(37, defaultArray4);
        ArrayList<Utf8> defaultArray5 = new ArrayList<Utf8>();
        TestRecord.put(38, defaultArray5);
        return TestRecord;
    }

    public com.linkedin.avro.fastserde.generated.avro.SubRecord deserializeSubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.SubRecord SubRecord;
        if ((reuse)!= null) {
            SubRecord = ((com.linkedin.avro.fastserde.generated.avro.SubRecord)(reuse));
        } else {
            SubRecord = new com.linkedin.avro.fastserde.generated.avro.SubRecord();
        }
        int unionIndex13 = (decoder.readIndex());
        switch (unionIndex13) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString2 = SubRecord.get(0);
                if (oldString2 instanceof Utf8) {
                    SubRecord.put(0, (decoder).readString(((Utf8) oldString2)));
                } else {
                    SubRecord.put(0, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex13));
        }
        int unionIndex14 = (decoder.readIndex());
        switch (unionIndex14) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                decoder.skipString();
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'removedField': "+ unionIndex14));
        }
        int unionIndex15 = (decoder.readIndex());
        switch (unionIndex15) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString3 = SubRecord.get(1);
                if (oldString3 instanceof Utf8) {
                    SubRecord.put(1, (decoder).readString(((Utf8) oldString3)));
                } else {
                    SubRecord.put(1, (decoder).readString(null));
                }
                break;
            }
            default:
                throw new RuntimeException(("Illegal union index for 'anotherField': "+ unionIndex15));
        }
        return SubRecord;
    }

}
