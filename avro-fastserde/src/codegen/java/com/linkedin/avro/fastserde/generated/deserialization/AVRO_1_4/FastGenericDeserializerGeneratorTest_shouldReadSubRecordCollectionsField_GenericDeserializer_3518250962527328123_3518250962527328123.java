
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField_GenericDeserializer_3518250962527328123_3518250962527328123
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema recordsArray0;
    private final Schema recordsArrayArrayElemSchema0;
    private final Schema subField0;
    private final Schema recordsMap0;
    private final Schema recordsArrayUnion0;
    private final Schema recordsArrayUnionOptionSchema0;
    private final Schema recordsArrayUnionOptionArrayElemSchema0;
    private final Schema recordsMapUnion0;
    private final Schema recordsMapUnionOptionSchema0;
    private final Schema recordsMapUnionOptionMapValueSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField_GenericDeserializer_3518250962527328123_3518250962527328123(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordsArray0 = readerSchema.getField("recordsArray").schema();
        this.recordsArrayArrayElemSchema0 = recordsArray0 .getElementType();
        this.subField0 = recordsArrayArrayElemSchema0 .getField("subField").schema();
        this.recordsMap0 = readerSchema.getField("recordsMap").schema();
        this.recordsArrayUnion0 = readerSchema.getField("recordsArrayUnion").schema();
        this.recordsArrayUnionOptionSchema0 = recordsArrayUnion0 .getTypes().get(1);
        this.recordsArrayUnionOptionArrayElemSchema0 = recordsArrayUnionOptionSchema0 .getElementType();
        this.recordsMapUnion0 = readerSchema.getField("recordsMapUnion").schema();
        this.recordsMapUnionOptionSchema0 = recordsMapUnion0 .getTypes().get(1);
        this.recordsMapUnionOptionMapValueSchema0 = recordsMapUnionOptionSchema0 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<IndexedRecord> recordsArray1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0);
        if (oldArray0 instanceof List) {
            recordsArray1 = ((List) oldArray0);
            recordsArray1 .clear();
        } else {
            recordsArray1 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen0), recordsArray0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object recordsArrayArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    recordsArrayArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                recordsArray1 .add(deserializesubRecord0(recordsArrayArrayElementReuseVar0, (decoder)));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(0, recordsArray1);
        populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField0((FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField), (decoder));
        populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField1((FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField), (decoder));
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordsArrayArrayElemSchema0)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordsArrayArrayElemSchema0);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                Object oldString0 = subRecord.get(0);
                if (oldString0 instanceof Utf8) {
                    subRecord.put(0, (decoder).readString(((Utf8) oldString0)));
                } else {
                    subRecord.put(0, (decoder).readString(null));
                }
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex0));
            }
        }
        return subRecord;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField0(IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField, Decoder decoder)
        throws IOException
    {
        Map<Utf8, IndexedRecord> recordsMap1 = null;
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            Map<Utf8, IndexedRecord> recordsMapReuse0 = null;
            Object oldMap0 = FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(1);
            if (oldMap0 instanceof Map) {
                recordsMapReuse0 = ((Map) oldMap0);
            }
            if (recordsMapReuse0 != (null)) {
                recordsMapReuse0 .clear();
                recordsMap1 = recordsMapReuse0;
            } else {
                recordsMap1 = new HashMap<Utf8, IndexedRecord>(((int)(((chunkLen1 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    recordsMap1 .put(key0, deserializesubRecord0(null, (decoder)));
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        } else {
            recordsMap1 = new HashMap<Utf8, IndexedRecord>(0);
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(1, recordsMap1);
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                List<IndexedRecord> recordsArrayUnionOption0 = null;
                long chunkLen2 = (decoder.readArrayStart());
                Object oldArray1 = FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2);
                if (oldArray1 instanceof List) {
                    recordsArrayUnionOption0 = ((List) oldArray1);
                    recordsArrayUnionOption0 .clear();
                } else {
                    recordsArrayUnionOption0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen2), recordsArrayUnionOptionSchema0);
                }
                while (chunkLen2 > 0) {
                    for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                        Object recordsArrayUnionOptionArrayElementReuseVar0 = null;
                        if (oldArray1 instanceof GenericArray) {
                            recordsArrayUnionOptionArrayElementReuseVar0 = ((GenericArray) oldArray1).peek();
                        }
                        int unionIndex2 = (decoder.readIndex());
                        if (unionIndex2 == 0) {
                            decoder.readNull();
                        } else {
                            if (unionIndex2 == 1) {
                                recordsArrayUnionOption0 .add(deserializesubRecord0(recordsArrayUnionOptionArrayElementReuseVar0, (decoder)));
                            } else {
                                throw new RuntimeException(("Illegal union index for 'recordsArrayUnionOptionElem': "+ unionIndex2));
                            }
                        }
                    }
                    chunkLen2 = (decoder.arrayNext());
                }
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(2, recordsArrayUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsArrayUnion': "+ unionIndex1));
            }
        }
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField1(IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex3 == 1) {
                Map<Utf8, IndexedRecord> recordsMapUnionOption0 = null;
                long chunkLen3 = (decoder.readMapStart());
                if (chunkLen3 > 0) {
                    Map<Utf8, IndexedRecord> recordsMapUnionOptionReuse0 = null;
                    Object oldMap1 = FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(3);
                    if (oldMap1 instanceof Map) {
                        recordsMapUnionOptionReuse0 = ((Map) oldMap1);
                    }
                    if (recordsMapUnionOptionReuse0 != (null)) {
                        recordsMapUnionOptionReuse0 .clear();
                        recordsMapUnionOption0 = recordsMapUnionOptionReuse0;
                    } else {
                        recordsMapUnionOption0 = new HashMap<Utf8, IndexedRecord>(((int)(((chunkLen3 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter3 = 0; (counter3 <chunkLen3); counter3 ++) {
                            Utf8 key1 = (decoder.readString(null));
                            int unionIndex4 = (decoder.readIndex());
                            if (unionIndex4 == 0) {
                                decoder.readNull();
                            } else {
                                if (unionIndex4 == 1) {
                                    recordsMapUnionOption0 .put(key1, deserializesubRecord0(null, (decoder)));
                                } else {
                                    throw new RuntimeException(("Illegal union index for 'recordsMapUnionOptionValue': "+ unionIndex4));
                                }
                            }
                        }
                        chunkLen3 = (decoder.mapNext());
                    } while (chunkLen3 > 0);
                } else {
                    recordsMapUnionOption0 = new HashMap<Utf8, IndexedRecord>(0);
                }
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(3, recordsMapUnionOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'recordsMapUnion': "+ unionIndex3));
            }
        }
    }

}
