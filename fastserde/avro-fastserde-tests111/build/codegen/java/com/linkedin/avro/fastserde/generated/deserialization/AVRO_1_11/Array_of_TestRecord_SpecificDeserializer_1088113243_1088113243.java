
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveBooleanList;
import com.linkedin.avro.api.PrimitiveDoubleList;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.SubRecord;
import com.linkedin.avro.fastserde.generated.avro.TestEnum;
import com.linkedin.avro.fastserde.generated.avro.TestFixed;
import com.linkedin.avro.fastserde.generated.avro.TestRecord;
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

public class Array_of_TestRecord_SpecificDeserializer_1088113243_1088113243
    implements FastDeserializer<List<TestRecord>>
{

    private final Schema readerSchema;

    public Array_of_TestRecord_SpecificDeserializer_1088113243_1088113243(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<TestRecord> deserialize(List<TestRecord> reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        List<TestRecord> array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if ((reuse) instanceof List) {
            array0 = ((List)(reuse));
            if (array0 instanceof GenericArray) {
                ((GenericArray) array0).reset();
            } else {
                array0 .clear();
            }
        } else {
            array0 = new ArrayList<TestRecord>(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object arrayArrayElementReuseVar0 = null;
                if ((reuse) instanceof GenericArray) {
                    arrayArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                }
                array0 .add(deserializeTestRecord0(arrayArrayElementReuseVar0, (decoder), (customization)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        return array0;
    }

    public TestRecord deserializeTestRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        TestRecord testRecord0;
        if ((reuse)!= null) {
            testRecord0 = ((TestRecord)(reuse));
        } else {
            testRecord0 = new TestRecord();
        }
        testRecord0 .put(0, (decoder.readInt()));
        populate_TestRecord0((testRecord0), (customization), (decoder));
        populate_TestRecord1((testRecord0), (customization), (decoder));
        populate_TestRecord2((testRecord0), (customization), (decoder));
        populate_TestRecord3((testRecord0), (customization), (decoder));
        populate_TestRecord4((testRecord0), (customization), (decoder));
        populate_TestRecord5((testRecord0), (customization), (decoder));
        populate_TestRecord6((testRecord0), (customization), (decoder));
        populate_TestRecord7((testRecord0), (customization), (decoder));
        populate_TestRecord8((testRecord0), (customization), (decoder));
        populate_TestRecord9((testRecord0), (customization), (decoder));
        populate_TestRecord10((testRecord0), (customization), (decoder));
        populate_TestRecord11((testRecord0), (customization), (decoder));
        populate_TestRecord12((testRecord0), (customization), (decoder));
        populate_TestRecord13((testRecord0), (customization), (decoder));
        populate_TestRecord14((testRecord0), (customization), (decoder));
        populate_TestRecord15((testRecord0), (customization), (decoder));
        populate_TestRecord16((testRecord0), (customization), (decoder));
        populate_TestRecord17((testRecord0), (customization), (decoder));
        populate_TestRecord18((testRecord0), (customization), (decoder));
        return testRecord0;
    }

    private void populate_TestRecord0(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            testRecord0 .put(1, null);
        } else {
            if (unionIndex0 == 1) {
                testRecord0 .put(1, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testIntUnion': "+ unionIndex0));
            }
        }
        testRecord0 .put(2, (decoder.readLong()));
    }

    private void populate_TestRecord1(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            testRecord0 .put(3, null);
        } else {
            if (unionIndex1 == 1) {
                testRecord0 .put(3, (decoder.readLong()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testLongUnion': "+ unionIndex1));
            }
        }
        testRecord0 .put(4, (decoder.readDouble()));
    }

    private void populate_TestRecord2(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            testRecord0 .put(5, null);
        } else {
            if (unionIndex2 == 1) {
                testRecord0 .put(5, (decoder.readDouble()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testDoubleUnion': "+ unionIndex2));
            }
        }
        testRecord0 .put(6, (decoder.readFloat()));
    }

    private void populate_TestRecord3(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
            testRecord0 .put(7, null);
        } else {
            if (unionIndex3 == 1) {
                testRecord0 .put(7, (decoder.readFloat()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testFloatUnion': "+ unionIndex3));
            }
        }
        testRecord0 .put(8, (decoder.readBoolean()));
    }

    private void populate_TestRecord4(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
            testRecord0 .put(9, null);
        } else {
            if (unionIndex4 == 1) {
                testRecord0 .put(9, (decoder.readBoolean()));
            } else {
                throw new RuntimeException(("Illegal union index for 'testBooleanUnion': "+ unionIndex4));
            }
        }
        ByteBuffer byteBuffer0;
        Object oldBytes0 = testRecord0 .get(10);
        if (oldBytes0 instanceof ByteBuffer) {
            byteBuffer0 = (decoder).readBytes(((ByteBuffer) oldBytes0));
        } else {
            byteBuffer0 = (decoder).readBytes((null));
        }
        testRecord0 .put(10, byteBuffer0);
    }

    private void populate_TestRecord5(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
            testRecord0 .put(11, null);
        } else {
            if (unionIndex5 == 1) {
                ByteBuffer byteBuffer1;
                Object oldBytes1 = testRecord0 .get(11);
                if (oldBytes1 instanceof ByteBuffer) {
                    byteBuffer1 = (decoder).readBytes(((ByteBuffer) oldBytes1));
                } else {
                    byteBuffer1 = (decoder).readBytes((null));
                }
                testRecord0 .put(11, byteBuffer1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testBytesUnion': "+ unionIndex5));
            }
        }
        Utf8 charSequence0;
        Object oldString0 = testRecord0 .get(12);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        testRecord0 .put(12, charSequence0);
    }

    private void populate_TestRecord6(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex6 = (decoder.readIndex());
        if (unionIndex6 == 0) {
            decoder.readNull();
            testRecord0 .put(13, null);
        } else {
            if (unionIndex6 == 1) {
                Utf8 charSequence1;
                Object oldString1 = testRecord0 .get(13);
                if (oldString1 instanceof Utf8) {
                    charSequence1 = (decoder).readString(((Utf8) oldString1));
                } else {
                    charSequence1 = (decoder).readString(null);
                }
                testRecord0 .put(13, charSequence1);
            } else {
                throw new RuntimeException(("Illegal union index for 'testStringUnion': "+ unionIndex6));
            }
        }
        byte[] testFixed0;
        Object oldFixed0 = testRecord0 .get(14);
        if ((oldFixed0 instanceof GenericFixed)&&(((GenericFixed) oldFixed0).bytes().length == (1))) {
            testFixed0 = ((GenericFixed) oldFixed0).bytes();
        } else {
            testFixed0 = ( new byte[1]);
        }
        decoder.readFixed(testFixed0);
        TestFixed testFixed1 = new TestFixed();
        testFixed1.bytes(testFixed0);
        testRecord0 .put(14, testFixed1);
    }

    private void populate_TestRecord7(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex7 = (decoder.readIndex());
        if (unionIndex7 == 0) {
            decoder.readNull();
            testRecord0 .put(15, null);
        } else {
            if (unionIndex7 == 1) {
                byte[] testFixed2;
                Object oldFixed1 = testRecord0 .get(15);
                if ((oldFixed1 instanceof GenericFixed)&&(((GenericFixed) oldFixed1).bytes().length == (1))) {
                    testFixed2 = ((GenericFixed) oldFixed1).bytes();
                } else {
                    testFixed2 = ( new byte[1]);
                }
                decoder.readFixed(testFixed2);
                TestFixed testFixed3 = new TestFixed();
                testFixed3.bytes(testFixed2);
                testRecord0 .put(15, testFixed3);
            } else {
                throw new RuntimeException(("Illegal union index for 'testFixedUnion': "+ unionIndex7));
            }
        }
        List<GenericFixed> testFixedArray0 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray0 = testRecord0 .get(16);
        if (oldArray0 instanceof List) {
            testFixedArray0 = ((List) oldArray0);
            if (testFixedArray0 instanceof GenericArray) {
                ((GenericArray) testFixedArray0).reset();
            } else {
                testFixedArray0 .clear();
            }
        } else {
            testFixedArray0 = new ArrayList<GenericFixed>(((int) chunkLen1));
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
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
            chunkLen1 = (decoder.arrayNext());
        }
        testRecord0 .put(16, testFixedArray0);
    }

    private void populate_TestRecord8(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        List<GenericFixed> testFixedUnionArray0 = null;
        long chunkLen2 = (decoder.readArrayStart());
        Object oldArray1 = testRecord0 .get(17);
        if (oldArray1 instanceof List) {
            testFixedUnionArray0 = ((List) oldArray1);
            if (testFixedUnionArray0 instanceof GenericArray) {
                ((GenericArray) testFixedUnionArray0).reset();
            } else {
                testFixedUnionArray0 .clear();
            }
        } else {
            testFixedUnionArray0 = new ArrayList<GenericFixed>(((int) chunkLen2));
        }
        while (chunkLen2 > 0) {
            for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
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
            chunkLen2 = (decoder.arrayNext());
        }
        testRecord0 .put(17, testFixedUnionArray0);
        testRecord0 .put(18, Enums.getConstant(TestEnum.class, (decoder.readEnum())));
    }

    private void populate_TestRecord9(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex9 = (decoder.readIndex());
        if (unionIndex9 == 0) {
            decoder.readNull();
            testRecord0 .put(19, null);
        } else {
            if (unionIndex9 == 1) {
                testRecord0 .put(19, Enums.getConstant(TestEnum.class, (decoder.readEnum())));
            } else {
                throw new RuntimeException(("Illegal union index for 'testEnumUnion': "+ unionIndex9));
            }
        }
        List<TestEnum> testEnumArray0 = null;
        long chunkLen3 = (decoder.readArrayStart());
        Object oldArray2 = testRecord0 .get(20);
        if (oldArray2 instanceof List) {
            testEnumArray0 = ((List) oldArray2);
            if (testEnumArray0 instanceof GenericArray) {
                ((GenericArray) testEnumArray0).reset();
            } else {
                testEnumArray0 .clear();
            }
        } else {
            testEnumArray0 = new ArrayList<TestEnum>(((int) chunkLen3));
        }
        while (chunkLen3 > 0) {
            for (int counter3 = 0; (counter3 <chunkLen3); counter3 ++) {
                testEnumArray0 .add(Enums.getConstant(TestEnum.class, (decoder.readEnum())));
            }
            chunkLen3 = (decoder.arrayNext());
        }
        testRecord0 .put(20, testEnumArray0);
    }

    private void populate_TestRecord10(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        List<TestEnum> testEnumUnionArray0 = null;
        long chunkLen4 = (decoder.readArrayStart());
        Object oldArray3 = testRecord0 .get(21);
        if (oldArray3 instanceof List) {
            testEnumUnionArray0 = ((List) oldArray3);
            if (testEnumUnionArray0 instanceof GenericArray) {
                ((GenericArray) testEnumUnionArray0).reset();
            } else {
                testEnumUnionArray0 .clear();
            }
        } else {
            testEnumUnionArray0 = new ArrayList<TestEnum>(((int) chunkLen4));
        }
        while (chunkLen4 > 0) {
            for (int counter4 = 0; (counter4 <chunkLen4); counter4 ++) {
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
            chunkLen4 = (decoder.arrayNext());
        }
        testRecord0 .put(21, testEnumUnionArray0);
        int unionIndex11 = (decoder.readIndex());
        if (unionIndex11 == 0) {
            decoder.readNull();
            testRecord0 .put(22, null);
        } else {
            if (unionIndex11 == 1) {
                testRecord0 .put(22, deserializeSubRecord0(testRecord0 .get(22), (decoder), (customization)));
            } else {
                throw new RuntimeException(("Illegal union index for 'subRecordUnion': "+ unionIndex11));
            }
        }
    }

    public SubRecord deserializeSubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        SubRecord subRecord0;
        if ((reuse)!= null) {
            subRecord0 = ((SubRecord)(reuse));
        } else {
            subRecord0 = new SubRecord();
        }
        int unionIndex12 = (decoder.readIndex());
        if (unionIndex12 == 0) {
            decoder.readNull();
            subRecord0 .put(0, null);
        } else {
            if (unionIndex12 == 1) {
                Utf8 charSequence2;
                Object oldString2 = subRecord0 .get(0);
                if (oldString2 instanceof Utf8) {
                    charSequence2 = (decoder).readString(((Utf8) oldString2));
                } else {
                    charSequence2 = (decoder).readString(null);
                }
                subRecord0 .put(0, charSequence2);
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex12));
            }
        }
        populate_SubRecord0((subRecord0), (customization), (decoder));
        return subRecord0;
    }

    private void populate_SubRecord0(SubRecord subRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex13 = (decoder.readIndex());
        if (unionIndex13 == 0) {
            decoder.readNull();
            subRecord0 .put(1, null);
        } else {
            if (unionIndex13 == 1) {
                Utf8 charSequence3;
                Object oldString3 = subRecord0 .get(1);
                if (oldString3 instanceof Utf8) {
                    charSequence3 = (decoder).readString(((Utf8) oldString3));
                } else {
                    charSequence3 = (decoder).readString(null);
                }
                subRecord0 .put(1, charSequence3);
            } else {
                throw new RuntimeException(("Illegal union index for 'anotherField': "+ unionIndex13));
            }
        }
    }

    private void populate_TestRecord11(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        testRecord0 .put(23, deserializeSubRecord0(testRecord0 .get(23), (decoder), (customization)));
        List<SubRecord> recordsArray0 = null;
        long chunkLen5 = (decoder.readArrayStart());
        Object oldArray4 = testRecord0 .get(24);
        if (oldArray4 instanceof List) {
            recordsArray0 = ((List) oldArray4);
            if (recordsArray0 instanceof GenericArray) {
                ((GenericArray) recordsArray0).reset();
            } else {
                recordsArray0 .clear();
            }
        } else {
            recordsArray0 = new ArrayList<SubRecord>(((int) chunkLen5));
        }
        while (chunkLen5 > 0) {
            for (int counter5 = 0; (counter5 <chunkLen5); counter5 ++) {
                Object recordsArrayArrayElementReuseVar0 = null;
                if (oldArray4 instanceof GenericArray) {
                    recordsArrayArrayElementReuseVar0 = ((GenericArray) oldArray4).peek();
                }
                recordsArray0 .add(deserializeSubRecord0(recordsArrayArrayElementReuseVar0, (decoder), (customization)));
            }
            chunkLen5 = (decoder.arrayNext());
        }
        testRecord0 .put(24, recordsArray0);
    }

    private void populate_TestRecord12(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        Map<Utf8, SubRecord> recordsMap0 = null;
        long chunkLen6 = (decoder.readMapStart());
        if (chunkLen6 > 0) {
            recordsMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(25), ((int) chunkLen6)));
            do {
                for (int counter6 = 0; (counter6 <chunkLen6); counter6 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    recordsMap0 .put(key0, deserializeSubRecord0(null, (decoder), (customization)));
                }
                chunkLen6 = (decoder.mapNext());
            } while (chunkLen6 > 0);
        } else {
            recordsMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(25), 0));
        }
        testRecord0 .put(25, recordsMap0);
        int unionIndex14 = (decoder.readIndex());
        if (unionIndex14 == 0) {
            decoder.readNull();
            testRecord0 .put(26, null);
        } else {
            if (unionIndex14 == 1) {
                List<SubRecord> recordsArrayUnionOption0 = null;
                long chunkLen7 = (decoder.readArrayStart());
                Object oldArray5 = testRecord0 .get(26);
                if (oldArray5 instanceof List) {
                    recordsArrayUnionOption0 = ((List) oldArray5);
                    if (recordsArrayUnionOption0 instanceof GenericArray) {
                        ((GenericArray) recordsArrayUnionOption0).reset();
                    } else {
                        recordsArrayUnionOption0 .clear();
                    }
                } else {
                    recordsArrayUnionOption0 = new ArrayList<SubRecord>(((int) chunkLen7));
                }
                while (chunkLen7 > 0) {
                    for (int counter7 = 0; (counter7 <chunkLen7); counter7 ++) {
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
                                recordsArrayUnionOption0 .add(deserializeSubRecord0(recordsArrayUnionOptionArrayElementReuseVar0, (decoder), (customization)));
                            } else {
                                throw new RuntimeException(("Illegal union index for 'recordsArrayUnionOptionElem': "+ unionIndex15));
                            }
                        }
                    }
                    chunkLen7 = (decoder.arrayNext());
                }
                testRecord0 .put(26, recordsArrayUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsArrayUnion': "+ unionIndex14));
            }
        }
    }

    private void populate_TestRecord13(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex16 = (decoder.readIndex());
        if (unionIndex16 == 0) {
            decoder.readNull();
            testRecord0 .put(27, null);
        } else {
            if (unionIndex16 == 1) {
                Map<Utf8, SubRecord> recordsMapUnionOption0 = null;
                long chunkLen8 = (decoder.readMapStart());
                if (chunkLen8 > 0) {
                    recordsMapUnionOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(27), ((int) chunkLen8)));
                    do {
                        for (int counter8 = 0; (counter8 <chunkLen8); counter8 ++) {
                            Utf8 key1 = (decoder.readString(null));
                            int unionIndex17 = (decoder.readIndex());
                            if (unionIndex17 == 0) {
                                decoder.readNull();
                                recordsMapUnionOption0 .put(key1, null);
                            } else {
                                if (unionIndex17 == 1) {
                                    recordsMapUnionOption0 .put(key1, deserializeSubRecord0(null, (decoder), (customization)));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'recordsMapUnionOptionValue': "+ unionIndex17));
                                }
                            }
                        }
                        chunkLen8 = (decoder.mapNext());
                    } while (chunkLen8 > 0);
                } else {
                    recordsMapUnionOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(27), 0));
                }
                testRecord0 .put(27, recordsMapUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsMapUnion': "+ unionIndex16));
            }
        }
        List<Map<Utf8, SubRecord>> recordsArrayMap0 = null;
        long chunkLen9 = (decoder.readArrayStart());
        Object oldArray6 = testRecord0 .get(28);
        if (oldArray6 instanceof List) {
            recordsArrayMap0 = ((List) oldArray6);
            if (recordsArrayMap0 instanceof GenericArray) {
                ((GenericArray) recordsArrayMap0).reset();
            } else {
                recordsArrayMap0 .clear();
            }
        } else {
            recordsArrayMap0 = new ArrayList<Map<Utf8, SubRecord>>(((int) chunkLen9));
        }
        while (chunkLen9 > 0) {
            for (int counter9 = 0; (counter9 <chunkLen9); counter9 ++) {
                Object recordsArrayMapArrayElementReuseVar0 = null;
                if (oldArray6 instanceof GenericArray) {
                    recordsArrayMapArrayElementReuseVar0 = ((GenericArray) oldArray6).peek();
                }
                Map<Utf8, SubRecord> recordsArrayMapElem0 = null;
                long chunkLen10 = (decoder.readMapStart());
                if (chunkLen10 > 0) {
                    recordsArrayMapElem0 = ((Map)(customization).getNewMapOverrideFunc().apply(recordsArrayMapArrayElementReuseVar0, ((int) chunkLen10)));
                    do {
                        for (int counter10 = 0; (counter10 <chunkLen10); counter10 ++) {
                            Utf8 key2 = (decoder.readString(null));
                            int unionIndex18 = (decoder.readIndex());
                            if (unionIndex18 == 0) {
                                decoder.readNull();
                                recordsArrayMapElem0 .put(key2, null);
                            } else {
                                if (unionIndex18 == 1) {
                                    recordsArrayMapElem0 .put(key2, deserializeSubRecord0(null, (decoder), (customization)));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'recordsArrayMapElemValue': "+ unionIndex18));
                                }
                            }
                        }
                        chunkLen10 = (decoder.mapNext());
                    } while (chunkLen10 > 0);
                } else {
                    recordsArrayMapElem0 = ((Map)(customization).getNewMapOverrideFunc().apply(recordsArrayMapArrayElementReuseVar0, 0));
                }
                recordsArrayMap0 .add(recordsArrayMapElem0);
            }
            chunkLen9 = (decoder.arrayNext());
        }
        testRecord0 .put(28, recordsArrayMap0);
    }

    private void populate_TestRecord14(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        Map<Utf8, List<SubRecord>> recordsMapArray0 = null;
        long chunkLen11 = (decoder.readMapStart());
        if (chunkLen11 > 0) {
            recordsMapArray0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(29), ((int) chunkLen11)));
            do {
                for (int counter11 = 0; (counter11 <chunkLen11); counter11 ++) {
                    Utf8 key3 = (decoder.readString(null));
                    List<SubRecord> recordsMapArrayValue0 = null;
                    long chunkLen12 = (decoder.readArrayStart());
                    if (null instanceof List) {
                        recordsMapArrayValue0 = ((List) null);
                        if (recordsMapArrayValue0 instanceof GenericArray) {
                            ((GenericArray) recordsMapArrayValue0).reset();
                        } else {
                            recordsMapArrayValue0 .clear();
                        }
                    } else {
                        recordsMapArrayValue0 = new ArrayList<SubRecord>(((int) chunkLen12));
                    }
                    while (chunkLen12 > 0) {
                        for (int counter12 = 0; (counter12 <chunkLen12); counter12 ++) {
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
                                    recordsMapArrayValue0 .add(deserializeSubRecord0(recordsMapArrayValueArrayElementReuseVar0, (decoder), (customization)));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'recordsMapArrayValueElem': "+ unionIndex19));
                                }
                            }
                        }
                        chunkLen12 = (decoder.arrayNext());
                    }
                    recordsMapArray0 .put(key3, recordsMapArrayValue0);
                }
                chunkLen11 = (decoder.mapNext());
            } while (chunkLen11 > 0);
        } else {
            recordsMapArray0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(29), 0));
        }
        testRecord0 .put(29, recordsMapArray0);
        int unionIndex20 = (decoder.readIndex());
        if (unionIndex20 == 0) {
            decoder.readNull();
            testRecord0 .put(30, null);
        } else {
            if (unionIndex20 == 1) {
                List<Map<Utf8, SubRecord>> recordsArrayMapUnionOption0 = null;
                long chunkLen13 = (decoder.readArrayStart());
                Object oldArray7 = testRecord0 .get(30);
                if (oldArray7 instanceof List) {
                    recordsArrayMapUnionOption0 = ((List) oldArray7);
                    if (recordsArrayMapUnionOption0 instanceof GenericArray) {
                        ((GenericArray) recordsArrayMapUnionOption0).reset();
                    } else {
                        recordsArrayMapUnionOption0 .clear();
                    }
                } else {
                    recordsArrayMapUnionOption0 = new ArrayList<Map<Utf8, SubRecord>>(((int) chunkLen13));
                }
                while (chunkLen13 > 0) {
                    for (int counter13 = 0; (counter13 <chunkLen13); counter13 ++) {
                        Object recordsArrayMapUnionOptionArrayElementReuseVar0 = null;
                        if (oldArray7 instanceof GenericArray) {
                            recordsArrayMapUnionOptionArrayElementReuseVar0 = ((GenericArray) oldArray7).peek();
                        }
                        Map<Utf8, SubRecord> recordsArrayMapUnionOptionElem0 = null;
                        long chunkLen14 = (decoder.readMapStart());
                        if (chunkLen14 > 0) {
                            recordsArrayMapUnionOptionElem0 = ((Map)(customization).getNewMapOverrideFunc().apply(recordsArrayMapUnionOptionArrayElementReuseVar0, ((int) chunkLen14)));
                            do {
                                for (int counter14 = 0; (counter14 <chunkLen14); counter14 ++) {
                                    Utf8 key4 = (decoder.readString(null));
                                    int unionIndex21 = (decoder.readIndex());
                                    if (unionIndex21 == 0) {
                                        decoder.readNull();
                                        recordsArrayMapUnionOptionElem0 .put(key4, null);
                                    } else {
                                        if (unionIndex21 == 1) {
                                            recordsArrayMapUnionOptionElem0 .put(key4, deserializeSubRecord0(null, (decoder), (customization)));
                                        } else {
                                            throw new RuntimeException(("Illegal union index for 'recordsArrayMapUnionOptionElemValue': "+ unionIndex21));
                                        }
                                    }
                                }
                                chunkLen14 = (decoder.mapNext());
                            } while (chunkLen14 > 0);
                        } else {
                            recordsArrayMapUnionOptionElem0 = ((Map)(customization).getNewMapOverrideFunc().apply(recordsArrayMapUnionOptionArrayElementReuseVar0, 0));
                        }
                        recordsArrayMapUnionOption0 .add(recordsArrayMapUnionOptionElem0);
                    }
                    chunkLen13 = (decoder.arrayNext());
                }
                testRecord0 .put(30, recordsArrayMapUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsArrayMapUnion': "+ unionIndex20));
            }
        }
    }

    private void populate_TestRecord15(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex22 = (decoder.readIndex());
        if (unionIndex22 == 0) {
            decoder.readNull();
            testRecord0 .put(31, null);
        } else {
            if (unionIndex22 == 1) {
                Map<Utf8, List<SubRecord>> recordsMapArrayUnionOption0 = null;
                long chunkLen15 = (decoder.readMapStart());
                if (chunkLen15 > 0) {
                    recordsMapArrayUnionOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(31), ((int) chunkLen15)));
                    do {
                        for (int counter15 = 0; (counter15 <chunkLen15); counter15 ++) {
                            Utf8 key5 = (decoder.readString(null));
                            List<SubRecord> recordsMapArrayUnionOptionValue0 = null;
                            long chunkLen16 = (decoder.readArrayStart());
                            if (null instanceof List) {
                                recordsMapArrayUnionOptionValue0 = ((List) null);
                                if (recordsMapArrayUnionOptionValue0 instanceof GenericArray) {
                                    ((GenericArray) recordsMapArrayUnionOptionValue0).reset();
                                } else {
                                    recordsMapArrayUnionOptionValue0 .clear();
                                }
                            } else {
                                recordsMapArrayUnionOptionValue0 = new ArrayList<SubRecord>(((int) chunkLen16));
                            }
                            while (chunkLen16 > 0) {
                                for (int counter16 = 0; (counter16 <chunkLen16); counter16 ++) {
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
                                            recordsMapArrayUnionOptionValue0 .add(deserializeSubRecord0(recordsMapArrayUnionOptionValueArrayElementReuseVar0, (decoder), (customization)));
                                        } else {
                                            throw new RuntimeException(("Illegal union index for 'recordsMapArrayUnionOptionValueElem': "+ unionIndex23));
                                        }
                                    }
                                }
                                chunkLen16 = (decoder.arrayNext());
                            }
                            recordsMapArrayUnionOption0 .put(key5, recordsMapArrayUnionOptionValue0);
                        }
                        chunkLen15 = (decoder.mapNext());
                    } while (chunkLen15 > 0);
                } else {
                    recordsMapArrayUnionOption0 = ((Map)(customization).getNewMapOverrideFunc().apply(testRecord0 .get(31), 0));
                }
                testRecord0 .put(31, recordsMapArrayUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsMapArrayUnion': "+ unionIndex22));
            }
        }
        int unionIndex24 = (decoder.readIndex());
        if (unionIndex24 == 0) {
            decoder.readNull();
            testRecord0 .put(32, null);
        } else {
            if (unionIndex24 == 1) {
                testRecord0 .put(32, deserializeSubRecord0(testRecord0 .get(32), (decoder), (customization)));
            } else {
                if (unionIndex24 == 2) {
                    Utf8 charSequence4;
                    Object oldString4 = testRecord0 .get(32);
                    if (oldString4 instanceof Utf8) {
                        charSequence4 = (decoder).readString(((Utf8) oldString4));
                    } else {
                        charSequence4 = (decoder).readString(null);
                    }
                    testRecord0 .put(32, charSequence4);
                } else {
                    if (unionIndex24 == 3) {
                        testRecord0 .put(32, (decoder.readInt()));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex24));
                    }
                }
            }
        }
    }

    private void populate_TestRecord16(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        PrimitiveBooleanList booleanArray0 = null;
        long chunkLen17 = (decoder.readArrayStart());
        Object oldArray8 = testRecord0 .get(33);
        if (oldArray8 instanceof PrimitiveBooleanList) {
            booleanArray0 = ((PrimitiveBooleanList) oldArray8);
            booleanArray0 .clear();
        } else {
            booleanArray0 = new PrimitiveBooleanArrayList(((int) chunkLen17));
        }
        while (chunkLen17 > 0) {
            for (int counter17 = 0; (counter17 <chunkLen17); counter17 ++) {
                booleanArray0 .addPrimitive((decoder.readBoolean()));
            }
            chunkLen17 = (decoder.arrayNext());
        }
        testRecord0 .put(33, booleanArray0);
        PrimitiveDoubleList doubleArray0 = null;
        long chunkLen18 = (decoder.readArrayStart());
        Object oldArray9 = testRecord0 .get(34);
        if (oldArray9 instanceof PrimitiveDoubleList) {
            doubleArray0 = ((PrimitiveDoubleList) oldArray9);
            doubleArray0 .clear();
        } else {
            doubleArray0 = new PrimitiveDoubleArrayList(((int) chunkLen18));
        }
        while (chunkLen18 > 0) {
            for (int counter18 = 0; (counter18 <chunkLen18); counter18 ++) {
                doubleArray0 .addPrimitive((decoder.readDouble()));
            }
            chunkLen18 = (decoder.arrayNext());
        }
        testRecord0 .put(34, doubleArray0);
    }

    private void populate_TestRecord17(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        PrimitiveFloatList floatArray0 = null;
        floatArray0 = ((PrimitiveFloatList) BufferBackedPrimitiveFloatList.readPrimitiveFloatArray(testRecord0 .get(35), (decoder)));
        testRecord0 .put(35, floatArray0);
        PrimitiveIntList intArray0 = null;
        long chunkLen19 = (decoder.readArrayStart());
        Object oldArray10 = testRecord0 .get(36);
        if (oldArray10 instanceof PrimitiveIntList) {
            intArray0 = ((PrimitiveIntList) oldArray10);
            intArray0 .clear();
        } else {
            intArray0 = new PrimitiveIntArrayList(((int) chunkLen19));
        }
        while (chunkLen19 > 0) {
            for (int counter19 = 0; (counter19 <chunkLen19); counter19 ++) {
                intArray0 .addPrimitive((decoder.readInt()));
            }
            chunkLen19 = (decoder.arrayNext());
        }
        testRecord0 .put(36, intArray0);
    }

    private void populate_TestRecord18(TestRecord testRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        PrimitiveLongList longArray0 = null;
        long chunkLen20 = (decoder.readArrayStart());
        Object oldArray11 = testRecord0 .get(37);
        if (oldArray11 instanceof PrimitiveLongList) {
            longArray0 = ((PrimitiveLongList) oldArray11);
            longArray0 .clear();
        } else {
            longArray0 = new PrimitiveLongArrayList(((int) chunkLen20));
        }
        while (chunkLen20 > 0) {
            for (int counter20 = 0; (counter20 <chunkLen20); counter20 ++) {
                longArray0 .addPrimitive((decoder.readLong()));
            }
            chunkLen20 = (decoder.arrayNext());
        }
        testRecord0 .put(37, longArray0);
        List<Utf8> stringArray0 = null;
        long chunkLen21 = (decoder.readArrayStart());
        Object oldArray12 = testRecord0 .get(38);
        if (oldArray12 instanceof List) {
            stringArray0 = ((List) oldArray12);
            if (stringArray0 instanceof GenericArray) {
                ((GenericArray) stringArray0).reset();
            } else {
                stringArray0 .clear();
            }
        } else {
            stringArray0 = new ArrayList<Utf8>(((int) chunkLen21));
        }
        while (chunkLen21 > 0) {
            for (int counter21 = 0; (counter21 <chunkLen21); counter21 ++) {
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
            chunkLen21 = (decoder.arrayNext());
        }
        testRecord0 .put(38, stringArray0);
    }

}
