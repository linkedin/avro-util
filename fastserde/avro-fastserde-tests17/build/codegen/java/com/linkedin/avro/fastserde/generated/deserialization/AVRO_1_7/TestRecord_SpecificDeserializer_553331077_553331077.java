
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveDoubleList;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import com.linkedin.avro.fastserde.primitive.PrimitiveBooleanArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveDoubleArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import com.linkedin.avroutil1.Enums;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class TestRecord_SpecificDeserializer_553331077_553331077
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.TestRecord>
{

    private final Schema readerSchema;

    public TestRecord_SpecificDeserializer_553331077_553331077(Schema readerSchema) {
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
        populate_TestRecord0((TestRecord), (decoder));
        populate_TestRecord1((TestRecord), (decoder));
        populate_TestRecord2((TestRecord), (decoder));
        populate_TestRecord3((TestRecord), (decoder));
        populate_TestRecord4((TestRecord), (decoder));
        populate_TestRecord5((TestRecord), (decoder));
        populate_TestRecord6((TestRecord), (decoder));
        populate_TestRecord7((TestRecord), (decoder));
        populate_TestRecord8((TestRecord), (decoder));
        populate_TestRecord9((TestRecord), (decoder));
        populate_TestRecord10((TestRecord), (decoder));
        populate_TestRecord11((TestRecord), (decoder));
        populate_TestRecord12((TestRecord), (decoder));
        populate_TestRecord13((TestRecord), (decoder));
        populate_TestRecord14((TestRecord), (decoder));
        populate_TestRecord15((TestRecord), (decoder));
        populate_TestRecord16((TestRecord), (decoder));
        populate_TestRecord17((TestRecord), (decoder));
        populate_TestRecord18((TestRecord), (decoder));
        return TestRecord;
    }

    private void populate_TestRecord0(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            TestRecord.put(1, null);
        } else {
            if (unionIndex0 == 1) {
                TestRecord.put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testIntUnion': "+ unionIndex0));
            }
        }
        TestRecord.put(2, (decoder.readLong()));
    }

    private void populate_TestRecord1(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            TestRecord.put(3, null);
        } else {
            if (unionIndex1 == 1) {
                TestRecord.put(3, (decoder.readLong()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testLongUnion': "+ unionIndex1));
            }
        }
        TestRecord.put(4, (decoder.readDouble()));
    }

    private void populate_TestRecord2(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            TestRecord.put(5, null);
        } else {
            if (unionIndex2 == 1) {
                TestRecord.put(5, (decoder.readDouble()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testDoubleUnion': "+ unionIndex2));
            }
        }
        TestRecord.put(6, (decoder.readFloat()));
    }

    private void populate_TestRecord3(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
            TestRecord.put(7, null);
        } else {
            if (unionIndex3 == 1) {
                TestRecord.put(7, (decoder.readFloat()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testFloatUnion': "+ unionIndex3));
            }
        }
        TestRecord.put(8, (decoder.readBoolean()));
    }

    private void populate_TestRecord4(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
            TestRecord.put(9, null);
        } else {
            if (unionIndex4 == 1) {
                TestRecord.put(9, (decoder.readBoolean()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testBooleanUnion': "+ unionIndex4));
            }
        }
        ByteBuffer byteBuffer0;
        Object oldBytes0 = TestRecord.get(10);
        if (oldBytes0 instanceof ByteBuffer) {
            byteBuffer0 = (decoder).readBytes(((ByteBuffer) oldBytes0));
        } else {
            byteBuffer0 = (decoder).readBytes((null));
        }
        TestRecord.put(10, byteBuffer0);
    }

    private void populate_TestRecord5(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
            TestRecord.put(11, null);
        } else {
            if (unionIndex5 == 1) {
                ByteBuffer byteBuffer1;
                Object oldBytes1 = TestRecord.get(11);
                if (oldBytes1 instanceof ByteBuffer) {
                    byteBuffer1 = (decoder).readBytes(((ByteBuffer) oldBytes1));
                } else {
                    byteBuffer1 = (decoder).readBytes((null));
                }
                TestRecord.put(11, byteBuffer1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testBytesUnion': "+ unionIndex5));
            }
        }
        Utf8 charSequence0;
        Object oldString0 = TestRecord.get(12);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        TestRecord.put(12, charSequence0);
    }

    private void populate_TestRecord6(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex6 = (decoder.readIndex());
        if (unionIndex6 == 0) {
            decoder.readNull();
            TestRecord.put(13, null);
        } else {
            if (unionIndex6 == 1) {
                Utf8 charSequence1;
                Object oldString1 = TestRecord.get(13);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                TestRecord.put(13, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testStringUnion': "+ unionIndex6));
            }
        }
        byte[] testFixed0;
        Object oldFixed0 = TestRecord.get(14);
        if ((oldFixed0 instanceof GenericFixed)&&(((GenericFixed) oldFixed0).bytes().length == (1))) {
            testFixed0 = ((GenericFixed) oldFixed0).bytes();
        } else {
            testFixed0 = ( new byte[1]);
        }
        decoder.readFixed(testFixed0);
        TestFixed testFixed1 = new TestFixed();
        testFixed1.bytes(testFixed0);
        TestRecord.put(14, testFixed1);
    }

    private void populate_TestRecord7(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex7 = (decoder.readIndex());
        if (unionIndex7 == 0) {
            decoder.readNull();
            TestRecord.put(15, null);
        } else {
            if (unionIndex7 == 1) {
                byte[] testFixed2;
                Object oldFixed1 = TestRecord.get(15);
                if ((oldFixed1 instanceof GenericFixed)&&(((GenericFixed) oldFixed1).bytes().length == (1))) {
                    testFixed2 = ((GenericFixed) oldFixed1).bytes();
                } else {
                    testFixed2 = ( new byte[1]);
                }
                decoder.readFixed(testFixed2);
                TestFixed testFixed3 = new TestFixed();
                testFixed3.bytes(testFixed2);
                TestRecord.put(15, testFixed3);
            } else {
                throw new RuntimeException(("Illegal union index for 'testFixedUnion': "+ unionIndex7));
            }
        }
        List<GenericFixed> testFixedArray0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = TestRecord.get(16);
        if (oldArray0 instanceof List) {
            testFixedArray0 = ((List) oldArray0);
            testFixedArray0 .clear();
        } else {
            testFixedArray0 = new ArrayList<GenericFixed>(((int) chunkLen0));
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
                TestFixed testFixed5 = new TestFixed();
                testFixed5.bytes(testFixed4);
                testFixedArray0 .add(testFixed5);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        TestRecord.put(16, testFixedArray0);
    }

    private void populate_TestRecord8(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        List<GenericFixed> testFixedUnionArray0 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray1 = TestRecord.get(17);
        if (oldArray1 instanceof List) {
            testFixedUnionArray0 = ((List) oldArray1);
            testFixedUnionArray0 .clear();
        } else {
            testFixedUnionArray0 = new ArrayList<GenericFixed>(((int) chunkLen1));
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                Object testFixedUnionArrayArrayElementReuseVar0 = null;
                if (oldArray1 instanceof GenericArray) {
                    testFixedUnionArrayArrayElementReuseVar0 = ((GenericArray) oldArray1).peek();
                }
                int unionIndex8 = (decoder.readIndex());
                if (unionIndex8 == 0) {
                    decoder.readNull();
                    testFixedUnionArray0 .add(null);
                } else {
                    if (unionIndex8 == 1) {
                        byte[] testFixed6;
                        Object oldFixed3 = testFixedUnionArrayArrayElementReuseVar0;
                        if ((oldFixed3 instanceof GenericFixed)&&(((GenericFixed) oldFixed3).bytes().length == (1))) {
                            testFixed6 = ((GenericFixed) oldFixed3).bytes();
                        } else {
                            testFixed6 = ( new byte[1]);
                        }
                        decoder.readFixed(testFixed6);
                        TestFixed testFixed7 = new TestFixed();
                        testFixed7.bytes(testFixed6);
                        testFixedUnionArray0 .add(testFixed7);
                    } else {
                        throw new RuntimeException(("Illegal union index for 'testFixedUnionArrayElem': "+ unionIndex8));
                    }
                }
            }
            chunkLen1 = (decoder.arrayNext());
        }
        TestRecord.put(17, testFixedUnionArray0);
        TestRecord.put(18, Enums.getConstant(TestEnum.class, (decoder.readEnum())));
    }

    private void populate_TestRecord9(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex9 = (decoder.readIndex());
        if (unionIndex9 == 0) {
            decoder.readNull();
            TestRecord.put(19, null);
        } else {
            if (unionIndex9 == 1) {
                TestRecord.put(19, Enums.getConstant(TestEnum.class, (decoder.readEnum())));
            } else {
                throw new RuntimeException(("Illegal union index for 'testEnumUnion': "+ unionIndex9));
            }
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
                testEnumArray0 .add(Enums.getConstant(TestEnum.class, (decoder.readEnum())));
            }
            chunkLen2 = (decoder.arrayNext());
        }
        TestRecord.put(20, testEnumArray0);
    }

    private void populate_TestRecord10(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
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
                int unionIndex10 = (decoder.readIndex());
                if (unionIndex10 == 0) {
                    decoder.readNull();
                    testEnumUnionArray0 .add(null);
                } else {
                    if (unionIndex10 == 1) {
                        testEnumUnionArray0 .add(Enums.getConstant(TestEnum.class, (decoder.readEnum())));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'testEnumUnionArrayElem': "+ unionIndex10));
                    }
                }
            }
            chunkLen3 = (decoder.arrayNext());
        }
        TestRecord.put(21, testEnumUnionArray0);
        int unionIndex11 = (decoder.readIndex());
        if (unionIndex11 == 0) {
            decoder.readNull();
            TestRecord.put(22, null);
        } else {
            if (unionIndex11 == 1) {
                TestRecord.put(22, deserializeSubRecord0(TestRecord.get(22), (decoder)));
            } else {
                throw new RuntimeException(("Illegal union index for 'subRecordUnion': "+ unionIndex11));
            }
        }
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
        int unionIndex12 = (decoder.readIndex());
        if (unionIndex12 == 0) {
            decoder.readNull();
            SubRecord.put(0, null);
        } else {
            if (unionIndex12 == 1) {
                Utf8 charSequence2;
                Object oldString2 = SubRecord.get(0);
                if (oldString2 instanceof Utf8) {
                    charSequence2 = (decoder).readString(((Utf8) oldString2));
                } else {
                    charSequence2 = (decoder).readString(null);
                }
                SubRecord.put(0, charSequence2);
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex12));
            }
        }
        populate_SubRecord0((SubRecord), (decoder));
        return SubRecord;
    }

    private void populate_SubRecord0(com.linkedin.avro.fastserde.generated.avro.SubRecord SubRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex13 = (decoder.readIndex());
        if (unionIndex13 == 0) {
            decoder.readNull();
            SubRecord.put(1, null);
        } else {
            if (unionIndex13 == 1) {
                Utf8 charSequence3;
                Object oldString3 = SubRecord.get(1);
                if (oldString3 instanceof Utf8) {
                    charSequence3 = (decoder).readString(((Utf8) oldString3));
                } else {
                    charSequence3 = (decoder).readString(null);
                }
                SubRecord.put(1, charSequence3);
            } else {
                throw new RuntimeException(("Illegal union index for 'anotherField': "+ unionIndex13));
            }
        }
    }

    private void populate_TestRecord11(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
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
    }

    private void populate_TestRecord12(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
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
        int unionIndex14 = (decoder.readIndex());
        if (unionIndex14 == 0) {
            decoder.readNull();
            TestRecord.put(26, null);
        } else {
            if (unionIndex14 == 1) {
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
                        int unionIndex15 = (decoder.readIndex());
                        if (unionIndex15 == 0) {
                            decoder.readNull();
                            recordsArrayUnionOption0 .add(null);
                        } else {
                            if (unionIndex15 == 1) {
                                recordsArrayUnionOption0 .add(deserializeSubRecord0(recordsArrayUnionOptionArrayElementReuseVar0, (decoder)));
                            } else {
                                throw new RuntimeException(("Illegal union index for 'recordsArrayUnionOptionElem': "+ unionIndex15));
                            }
                        }
                    }
                    chunkLen6 = (decoder.arrayNext());
                }
                TestRecord.put(26, recordsArrayUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsArrayUnion': "+ unionIndex14));
            }
        }
    }

    private void populate_TestRecord13(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex16 = (decoder.readIndex());
        if (unionIndex16 == 0) {
            decoder.readNull();
            TestRecord.put(27, null);
        } else {
            if (unionIndex16 == 1) {
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
                            int unionIndex17 = (decoder.readIndex());
                            if (unionIndex17 == 0) {
                                decoder.readNull();
                                recordsMapUnionOption0 .put(key1, null);
                            } else {
                                if (unionIndex17 == 1) {
                                    recordsMapUnionOption0 .put(key1, deserializeSubRecord0(null, (decoder)));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'recordsMapUnionOptionValue': "+ unionIndex17));
                                }
                            }
                        }
                        chunkLen7 = (decoder.mapNext());
                    } while (chunkLen7 > 0);
                } else {
                    recordsMapUnionOption0 = new HashMap<Utf8, com.linkedin.avro.fastserde.generated.avro.SubRecord>(0);
                }
                TestRecord.put(27, recordsMapUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsMapUnion': "+ unionIndex16));
            }
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
                            int unionIndex18 = (decoder.readIndex());
                            if (unionIndex18 == 0) {
                                decoder.readNull();
                                recordsArrayMapElem0 .put(key2, null);
                            } else {
                                if (unionIndex18 == 1) {
                                    recordsArrayMapElem0 .put(key2, deserializeSubRecord0(null, (decoder)));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'recordsArrayMapElemValue': "+ unionIndex18));
                                }
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
    }

    private void populate_TestRecord14(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
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
                            int unionIndex19 = (decoder.readIndex());
                            if (unionIndex19 == 0) {
                                decoder.readNull();
                                recordsMapArrayValue0 .add(null);
                            } else {
                                if (unionIndex19 == 1) {
                                    recordsMapArrayValue0 .add(deserializeSubRecord0(recordsMapArrayValueArrayElementReuseVar0, (decoder)));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'recordsMapArrayValueElem': "+ unionIndex19));
                                }
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
        int unionIndex20 = (decoder.readIndex());
        if (unionIndex20 == 0) {
            decoder.readNull();
            TestRecord.put(30, null);
        } else {
            if (unionIndex20 == 1) {
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
                                    int unionIndex21 = (decoder.readIndex());
                                    if (unionIndex21 == 0) {
                                        decoder.readNull();
                                        recordsArrayMapUnionOptionElem0 .put(key4, null);
                                    } else {
                                        if (unionIndex21 == 1) {
                                            recordsArrayMapUnionOptionElem0 .put(key4, deserializeSubRecord0(null, (decoder)));
                                        } else {
                                            throw new RuntimeException(("Illegal union index for 'recordsArrayMapUnionOptionElemValue': "+ unionIndex21));
                                        }
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
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsArrayMapUnion': "+ unionIndex20));
            }
        }
    }

    private void populate_TestRecord15(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        int unionIndex22 = (decoder.readIndex());
        if (unionIndex22 == 0) {
            decoder.readNull();
            TestRecord.put(31, null);
        } else {
            if (unionIndex22 == 1) {
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
                                    int unionIndex23 = (decoder.readIndex());
                                    if (unionIndex23 == 0) {
                                        decoder.readNull();
                                        recordsMapArrayUnionOptionValue0 .add(null);
                                    } else {
                                        if (unionIndex23 == 1) {
                                            recordsMapArrayUnionOptionValue0 .add(deserializeSubRecord0(recordsMapArrayUnionOptionValueArrayElementReuseVar0, (decoder)));
                                        } else {
                                            throw new RuntimeException(("Illegal union index for 'recordsMapArrayUnionOptionValueElem': "+ unionIndex23));
                                        }
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
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsMapArrayUnion': "+ unionIndex22));
            }
        }
        int unionIndex24 = (decoder.readIndex());
        if (unionIndex24 == 0) {
            decoder.readNull();
            TestRecord.put(32, null);
        } else {
            if (unionIndex24 == 1) {
                TestRecord.put(32, deserializeSubRecord0(TestRecord.get(32), (decoder)));
            } else {
                if (unionIndex24 == 2) {
                    Utf8 charSequence4;
                    Object oldString4 = TestRecord.get(32);
                    if (oldString4 instanceof Utf8) {
                        charSequence4 = (decoder).readString(((Utf8) oldString4));
                    } else {
                        charSequence4 = (decoder).readString(null);
                    }
                    TestRecord.put(32, charSequence4);
                } else {
                    if (unionIndex24 == 3) {
                        TestRecord.put(32, (decoder.readInt()));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex24));
                    }
                }
            }
        }
    }

    private void populate_TestRecord16(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        PrimitiveBooleanList booleanArray0 = null;
        long chunkLen16 = (decoder.readArrayStart());
        Object oldArray8 = TestRecord.get(33);
        if (oldArray8 instanceof PrimitiveBooleanList) {
            booleanArray0 = ((PrimitiveBooleanList) oldArray8);
            booleanArray0 .clear();
        } else {
            booleanArray0 = new PrimitiveBooleanArrayList(((int) chunkLen16));
        }
        while (chunkLen16 > 0) {
            for (int counter16 = 0; (counter16 <chunkLen16); counter16 ++) {
                booleanArray0 .addPrimitive((decoder.readBoolean()));
            }
            chunkLen16 = (decoder.arrayNext());
        }
        TestRecord.put(33, booleanArray0);
        PrimitiveDoubleList doubleArray0 = null;
        long chunkLen17 = (decoder.readArrayStart());
        Object oldArray9 = TestRecord.get(34);
        if (oldArray9 instanceof PrimitiveDoubleList) {
            doubleArray0 = ((PrimitiveDoubleList) oldArray9);
            doubleArray0 .clear();
        } else {
            doubleArray0 = new PrimitiveDoubleArrayList(((int) chunkLen17));
        }
        while (chunkLen17 > 0) {
            for (int counter17 = 0; (counter17 <chunkLen17); counter17 ++) {
                doubleArray0 .addPrimitive((decoder.readDouble()));
            }
            chunkLen17 = (decoder.arrayNext());
        }
        TestRecord.put(34, doubleArray0);
    }

    private void populate_TestRecord17(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        PrimitiveFloatList floatArray0 = null;
        floatArray0 = ((PrimitiveFloatList) BufferBackedPrimitiveFloatList.readPrimitiveFloatArray(TestRecord.get(35), (decoder)));
        TestRecord.put(35, floatArray0);
        PrimitiveIntList intArray0 = null;
        long chunkLen18 = (decoder.readArrayStart());
        Object oldArray10 = TestRecord.get(36);
        if (oldArray10 instanceof PrimitiveIntList) {
            intArray0 = ((PrimitiveIntList) oldArray10);
            intArray0 .clear();
        } else {
            intArray0 = new PrimitiveIntArrayList(((int) chunkLen18));
        }
        while (chunkLen18 > 0) {
            for (int counter18 = 0; (counter18 <chunkLen18); counter18 ++) {
                intArray0 .addPrimitive((decoder.readInt()));
            }
            chunkLen18 = (decoder.arrayNext());
        }
        TestRecord.put(36, intArray0);
    }

    private void populate_TestRecord18(com.linkedin.avro.fastserde.generated.avro.TestRecord TestRecord, Decoder decoder)
        throws IOException
    {
        PrimitiveLongList longArray0 = null;
        long chunkLen19 = (decoder.readArrayStart());
        Object oldArray11 = TestRecord.get(37);
        if (oldArray11 instanceof PrimitiveLongList) {
            longArray0 = ((PrimitiveLongList) oldArray11);
            longArray0 .clear();
        } else {
            longArray0 = new PrimitiveLongArrayList(((int) chunkLen19));
        }
        while (chunkLen19 > 0) {
            for (int counter19 = 0; (counter19 <chunkLen19); counter19 ++) {
                longArray0 .addPrimitive((decoder.readLong()));
            }
            chunkLen19 = (decoder.arrayNext());
        }
        TestRecord.put(37, longArray0);
        List<Utf8> stringArray0 = null;
        long chunkLen20 = (decoder.readArrayStart());
        Object oldArray12 = TestRecord.get(38);
        if (oldArray12 instanceof List) {
            stringArray0 = ((List) oldArray12);
            stringArray0 .clear();
        } else {
            stringArray0 = new ArrayList<Utf8>(((int) chunkLen20));
        }
        while (chunkLen20 > 0) {
            for (int counter20 = 0; (counter20 <chunkLen20); counter20 ++) {
                Object stringArrayArrayElementReuseVar0 = null;
                if (oldArray12 instanceof GenericArray) {
                    stringArrayArrayElementReuseVar0 = ((GenericArray) oldArray12).peek();
                }
                Utf8 charSequence5;
                if (stringArrayArrayElementReuseVar0 instanceof Utf8) {
                    charSequence5 = (decoder).readString(((Utf8) stringArrayArrayElementReuseVar0));
                } else {
                    charSequence5 = (decoder).readString(null);
                }
                stringArray0 .add(charSequence5);
            }
            chunkLen20 = (decoder.arrayNext());
        }
        TestRecord.put(38, stringArray0);
    }

}
