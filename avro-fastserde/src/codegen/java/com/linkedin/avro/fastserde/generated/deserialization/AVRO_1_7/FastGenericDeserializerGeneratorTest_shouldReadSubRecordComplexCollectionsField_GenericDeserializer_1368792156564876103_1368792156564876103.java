
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField_GenericDeserializer_1368792156564876103_1368792156564876103
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema recordsArrayMap0;
    private final Schema recordsArrayMapArrayElemSchema0;
    private final Schema recordsArrayMapElemMapValueSchema0;
    private final Schema recordsArrayMapElemValueOptionSchema0;
    private final Schema subField0;
    private final Schema recordsMapArray0;
    private final Schema recordsMapArrayMapValueSchema0;
    private final Schema recordsMapArrayValueArrayElemSchema0;
    private final Schema recordsArrayMapUnion0;
    private final Schema recordsArrayMapUnionOptionArrayElemSchema0;
    private final Schema recordsArrayMapUnionOptionElemMapValueSchema0;
    private final Schema recordsMapArrayUnion0;
    private final Schema recordsMapArrayUnionOptionSchema0;
    private final Schema recordsMapArrayUnionOptionValueArrayElemSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField_GenericDeserializer_1368792156564876103_1368792156564876103(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordsArrayMap0 = readerSchema.getField("recordsArrayMap").schema();
        this.recordsArrayMapArrayElemSchema0 = recordsArrayMap0 .getElementType();
        this.recordsArrayMapElemMapValueSchema0 = recordsArrayMapArrayElemSchema0 .getValueType();
        this.recordsArrayMapElemValueOptionSchema0 = recordsArrayMapElemMapValueSchema0 .getTypes().get(1);
        this.subField0 = recordsArrayMapElemValueOptionSchema0 .getField("subField").schema();
        this.recordsMapArray0 = readerSchema.getField("recordsMapArray").schema();
        this.recordsMapArrayMapValueSchema0 = recordsMapArray0 .getValueType();
        this.recordsMapArrayValueArrayElemSchema0 = recordsMapArrayMapValueSchema0 .getElementType();
        this.recordsArrayMapUnion0 = readerSchema.getField("recordsArrayMapUnion").schema();
        this.recordsArrayMapUnionOptionArrayElemSchema0 = recordsArrayMap0 .getElementType();
        this.recordsArrayMapUnionOptionElemMapValueSchema0 = recordsArrayMapUnionOptionArrayElemSchema0 .getValueType();
        this.recordsMapArrayUnion0 = readerSchema.getField("recordsMapArrayUnion").schema();
        this.recordsMapArrayUnionOptionSchema0 = recordsMapArrayUnion0 .getTypes().get(1);
        this.recordsMapArrayUnionOptionValueArrayElemSchema0 = recordsMapArrayMapValueSchema0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<Map<Utf8, IndexedRecord>> recordsArrayMap1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (chunkLen0 > 0) {
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0) instanceof List) {
                recordsArrayMap1 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0));
                recordsArrayMap1 .clear();
            } else {
                recordsArrayMap1 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen0), recordsArrayMap0);
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Object recordsArrayMapArrayElementReuseVar0 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0) instanceof GenericArray) {
                        recordsArrayMapArrayElementReuseVar0 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0)).peek();
                    }
                    Map<Utf8, IndexedRecord> recordsArrayMapElem0 = null;
                    long chunkLen1 = (decoder.readMapStart());
                    if (chunkLen1 > 0) {
                        Map<Utf8, IndexedRecord> recordsArrayMapElemReuse0 = null;
                        if (recordsArrayMapArrayElementReuseVar0 instanceof Map) {
                            recordsArrayMapElemReuse0 = ((Map) recordsArrayMapArrayElementReuseVar0);
                        }
                        if (recordsArrayMapElemReuse0 != (null)) {
                            recordsArrayMapElemReuse0 .clear();
                            recordsArrayMapElem0 = recordsArrayMapElemReuse0;
                        } else {
                            recordsArrayMapElem0 = new HashMap<Utf8, IndexedRecord>();
                        }
                        do {
                            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                                Utf8 key0 = (decoder.readString(null));
                                int unionIndex0 = (decoder.readIndex());
                                if (unionIndex0 == 0) {
                                    decoder.readNull();
                                } else {
                                    if (unionIndex0 == 1) {
                                        recordsArrayMapElem0 .put(key0, deserializesubRecord0(null, (decoder)));
                                    }
                                }
                            }
                            chunkLen1 = (decoder.mapNext());
                        } while (chunkLen1 > 0);
                    } else {
                        recordsArrayMapElem0 = Collections.emptyMap();
                    }
                    recordsArrayMap1 .add(recordsArrayMapElem0);
                }
                chunkLen0 = (decoder.arrayNext());
            } while (chunkLen0 > 0);
        } else {
            recordsArrayMap1 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen0), recordsArrayMap0);
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(0, recordsArrayMap1);
        Map<Utf8, List<IndexedRecord>> recordsMapArray1 = null;
        long chunkLen2 = (decoder.readMapStart());
        if (chunkLen2 > 0) {
            Map<Utf8, List<IndexedRecord>> recordsMapArrayReuse0 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(1) instanceof Map) {
                recordsMapArrayReuse0 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(1));
            }
            if (recordsMapArrayReuse0 != (null)) {
                recordsMapArrayReuse0 .clear();
                recordsMapArray1 = recordsMapArrayReuse0;
            } else {
                recordsMapArray1 = new HashMap<Utf8, List<IndexedRecord>>();
            }
            do {
                for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                    Utf8 key1 = (decoder.readString(null));
                    List<IndexedRecord> recordsMapArrayValue0 = null;
                    long chunkLen3 = (decoder.readArrayStart());
                    if (chunkLen3 > 0) {
                        if (null instanceof List) {
                            recordsMapArrayValue0 = ((List) null);
                            recordsMapArrayValue0 .clear();
                        } else {
                            recordsMapArrayValue0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen3), recordsMapArrayMapValueSchema0);
                        }
                        do {
                            for (int counter3 = 0; (counter3 <chunkLen3); counter3 ++) {
                                Object recordsMapArrayValueArrayElementReuseVar0 = null;
                                if (null instanceof GenericArray) {
                                    recordsMapArrayValueArrayElementReuseVar0 = ((GenericArray) null).peek();
                                }
                                int unionIndex2 = (decoder.readIndex());
                                if (unionIndex2 == 0) {
                                    decoder.readNull();
                                } else {
                                    if (unionIndex2 == 1) {
                                        recordsMapArrayValue0 .add(deserializesubRecord0(recordsMapArrayValueArrayElementReuseVar0, (decoder)));
                                    }
                                }
                            }
                            chunkLen3 = (decoder.arrayNext());
                        } while (chunkLen3 > 0);
                    } else {
                        recordsMapArrayValue0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen3), recordsMapArrayMapValueSchema0);
                    }
                    recordsMapArray1 .put(key1, recordsMapArrayValue0);
                }
                chunkLen2 = (decoder.mapNext());
            } while (chunkLen2 > 0);
        } else {
            recordsMapArray1 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(1, recordsMapArray1);
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex3 == 1) {
                List<Map<Utf8, IndexedRecord>> recordsArrayMapUnionOption0 = null;
                long chunkLen4 = (decoder.readArrayStart());
                if (chunkLen4 > 0) {
                    if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2) instanceof List) {
                        recordsArrayMapUnionOption0 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2));
                        recordsArrayMapUnionOption0 .clear();
                    } else {
                        recordsArrayMapUnionOption0 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen4), recordsArrayMap0);
                    }
                    do {
                        for (int counter4 = 0; (counter4 <chunkLen4); counter4 ++) {
                            Object recordsArrayMapUnionOptionArrayElementReuseVar0 = null;
                            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2) instanceof GenericArray) {
                                recordsArrayMapUnionOptionArrayElementReuseVar0 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2)).peek();
                            }
                            Map<Utf8, IndexedRecord> recordsArrayMapUnionOptionElem0 = null;
                            long chunkLen5 = (decoder.readMapStart());
                            if (chunkLen5 > 0) {
                                Map<Utf8, IndexedRecord> recordsArrayMapUnionOptionElemReuse0 = null;
                                if (recordsArrayMapUnionOptionArrayElementReuseVar0 instanceof Map) {
                                    recordsArrayMapUnionOptionElemReuse0 = ((Map) recordsArrayMapUnionOptionArrayElementReuseVar0);
                                }
                                if (recordsArrayMapUnionOptionElemReuse0 != (null)) {
                                    recordsArrayMapUnionOptionElemReuse0 .clear();
                                    recordsArrayMapUnionOptionElem0 = recordsArrayMapUnionOptionElemReuse0;
                                } else {
                                    recordsArrayMapUnionOptionElem0 = new HashMap<Utf8, IndexedRecord>();
                                }
                                do {
                                    for (int counter5 = 0; (counter5 <chunkLen5); counter5 ++) {
                                        Utf8 key2 = (decoder.readString(null));
                                        int unionIndex4 = (decoder.readIndex());
                                        if (unionIndex4 == 0) {
                                            decoder.readNull();
                                        } else {
                                            if (unionIndex4 == 1) {
                                                recordsArrayMapUnionOptionElem0 .put(key2, deserializesubRecord0(null, (decoder)));
                                            }
                                        }
                                    }
                                    chunkLen5 = (decoder.mapNext());
                                } while (chunkLen5 > 0);
                            } else {
                                recordsArrayMapUnionOptionElem0 = Collections.emptyMap();
                            }
                            recordsArrayMapUnionOption0 .add(recordsArrayMapUnionOptionElem0);
                        }
                        chunkLen4 = (decoder.arrayNext());
                    } while (chunkLen4 > 0);
                } else {
                    recordsArrayMapUnionOption0 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen4), recordsArrayMap0);
                }
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(2, recordsArrayMapUnionOption0);
            }
        }
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex5 == 1) {
                Map<Utf8, List<IndexedRecord>> recordsMapArrayUnionOption0 = null;
                long chunkLen6 = (decoder.readMapStart());
                if (chunkLen6 > 0) {
                    Map<Utf8, List<IndexedRecord>> recordsMapArrayUnionOptionReuse0 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(3) instanceof Map) {
                        recordsMapArrayUnionOptionReuse0 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(3));
                    }
                    if (recordsMapArrayUnionOptionReuse0 != (null)) {
                        recordsMapArrayUnionOptionReuse0 .clear();
                        recordsMapArrayUnionOption0 = recordsMapArrayUnionOptionReuse0;
                    } else {
                        recordsMapArrayUnionOption0 = new HashMap<Utf8, List<IndexedRecord>>();
                    }
                    do {
                        for (int counter6 = 0; (counter6 <chunkLen6); counter6 ++) {
                            Utf8 key3 = (decoder.readString(null));
                            List<IndexedRecord> recordsMapArrayUnionOptionValue0 = null;
                            long chunkLen7 = (decoder.readArrayStart());
                            if (chunkLen7 > 0) {
                                if (null instanceof List) {
                                    recordsMapArrayUnionOptionValue0 = ((List) null);
                                    recordsMapArrayUnionOptionValue0 .clear();
                                } else {
                                    recordsMapArrayUnionOptionValue0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen7), recordsMapArrayMapValueSchema0);
                                }
                                do {
                                    for (int counter7 = 0; (counter7 <chunkLen7); counter7 ++) {
                                        Object recordsMapArrayUnionOptionValueArrayElementReuseVar0 = null;
                                        if (null instanceof GenericArray) {
                                            recordsMapArrayUnionOptionValueArrayElementReuseVar0 = ((GenericArray) null).peek();
                                        }
                                        int unionIndex6 = (decoder.readIndex());
                                        if (unionIndex6 == 0) {
                                            decoder.readNull();
                                        } else {
                                            if (unionIndex6 == 1) {
                                                recordsMapArrayUnionOptionValue0 .add(deserializesubRecord0(recordsMapArrayUnionOptionValueArrayElementReuseVar0, (decoder)));
                                            }
                                        }
                                    }
                                    chunkLen7 = (decoder.arrayNext());
                                } while (chunkLen7 > 0);
                            } else {
                                recordsMapArrayUnionOptionValue0 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen7), recordsMapArrayMapValueSchema0);
                            }
                            recordsMapArrayUnionOption0 .put(key3, recordsMapArrayUnionOptionValue0);
                        }
                        chunkLen6 = (decoder.mapNext());
                    } while (chunkLen6 > 0);
                } else {
                    recordsMapArrayUnionOption0 = Collections.emptyMap();
                }
                FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(3, recordsMapArrayUnionOption0);
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordsArrayMapElemValueOptionSchema0)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordsArrayMapElemValueOptionSchema0);
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                if (subRecord.get(0) instanceof Utf8) {
                    subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
                } else {
                    subRecord.put(0, (decoder).readString(null));
                }
            }
        }
        return subRecord;
    }

}
