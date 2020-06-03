
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

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

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField_GenericDeserializer_3807952864887349709_3807952864887349709
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema recordsArrayMap982;
    private final Schema recordsArrayMapArrayElemSchema987;
    private final Schema recordsArrayMapElemMapValueSchema994;
    private final Schema recordsArrayMapElemValueOptionSchema996;
    private final Schema subField998;
    private final Schema recordsMapArray1000;
    private final Schema recordsMapArrayMapValueSchema1006;
    private final Schema recordsMapArrayValueArrayElemSchema1011;
    private final Schema recordsArrayMapUnion1014;
    private final Schema recordsArrayMapUnionOptionArrayElemSchema1020;
    private final Schema recordsArrayMapUnionOptionElemMapValueSchema1027;
    private final Schema recordsMapArrayUnion1029;
    private final Schema recordsMapArrayUnionOptionSchema1031;
    private final Schema recordsMapArrayUnionOptionValueArrayElemSchema1041;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField_GenericDeserializer_3807952864887349709_3807952864887349709(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordsArrayMap982 = readerSchema.getField("recordsArrayMap").schema();
        this.recordsArrayMapArrayElemSchema987 = recordsArrayMap982 .getElementType();
        this.recordsArrayMapElemMapValueSchema994 = recordsArrayMapArrayElemSchema987 .getValueType();
        this.recordsArrayMapElemValueOptionSchema996 = recordsArrayMapElemMapValueSchema994 .getTypes().get(1);
        this.subField998 = recordsArrayMapElemValueOptionSchema996 .getField("subField").schema();
        this.recordsMapArray1000 = readerSchema.getField("recordsMapArray").schema();
        this.recordsMapArrayMapValueSchema1006 = recordsMapArray1000 .getValueType();
        this.recordsMapArrayValueArrayElemSchema1011 = recordsMapArrayMapValueSchema1006 .getElementType();
        this.recordsArrayMapUnion1014 = readerSchema.getField("recordsArrayMapUnion").schema();
        this.recordsArrayMapUnionOptionArrayElemSchema1020 = recordsArrayMap982 .getElementType();
        this.recordsArrayMapUnionOptionElemMapValueSchema1027 = recordsArrayMapUnionOptionArrayElemSchema1020 .getValueType();
        this.recordsMapArrayUnion1029 = readerSchema.getField("recordsMapArrayUnion").schema();
        this.recordsMapArrayUnionOptionSchema1031 = recordsMapArrayUnion1029 .getTypes().get(1);
        this.recordsMapArrayUnionOptionValueArrayElemSchema1041 = recordsMapArrayMapValueSchema1006 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField981((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField981(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<Map<Utf8, IndexedRecord>> recordsArrayMap983 = null;
        long chunkLen984 = (decoder.readArrayStart());
        if (chunkLen984 > 0) {
            List<Map<Utf8, IndexedRecord>> recordsArrayMapReuse985 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0) instanceof List) {
                recordsArrayMapReuse985 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0));
            }
            if (recordsArrayMapReuse985 != (null)) {
                recordsArrayMapReuse985 .clear();
                recordsArrayMap983 = recordsArrayMapReuse985;
            } else {
                recordsArrayMap983 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen984), recordsArrayMap982);
            }
            do {
                for (int counter986 = 0; (counter986 <chunkLen984); counter986 ++) {
                    Object recordsArrayMapArrayElementReuseVar988 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0) instanceof GenericArray) {
                        recordsArrayMapArrayElementReuseVar988 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0)).peek();
                    }
                    Map<Utf8, IndexedRecord> recordsArrayMapElem989 = null;
                    long chunkLen990 = (decoder.readMapStart());
                    if (chunkLen990 > 0) {
                        Map<Utf8, IndexedRecord> recordsArrayMapElemReuse991 = null;
                        if (recordsArrayMapArrayElementReuseVar988 instanceof Map) {
                            recordsArrayMapElemReuse991 = ((Map) recordsArrayMapArrayElementReuseVar988);
                        }
                        if (recordsArrayMapElemReuse991 != (null)) {
                            recordsArrayMapElemReuse991 .clear();
                            recordsArrayMapElem989 = recordsArrayMapElemReuse991;
                        } else {
                            recordsArrayMapElem989 = new HashMap<Utf8, IndexedRecord>();
                        }
                        do {
                            for (int counter992 = 0; (counter992 <chunkLen990); counter992 ++) {
                                Utf8 key993 = (decoder.readString(null));
                                int unionIndex995 = (decoder.readIndex());
                                if (unionIndex995 == 0) {
                                    decoder.readNull();
                                }
                                if (unionIndex995 == 1) {
                                    recordsArrayMapElem989 .put(key993, deserializesubRecord997(null, (decoder)));
                                }
                            }
                            chunkLen990 = (decoder.mapNext());
                        } while (chunkLen990 > 0);
                    } else {
                        recordsArrayMapElem989 = Collections.emptyMap();
                    }
                    recordsArrayMap983 .add(recordsArrayMapElem989);
                }
                chunkLen984 = (decoder.arrayNext());
            } while (chunkLen984 > 0);
        } else {
            recordsArrayMap983 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(0, recordsArrayMap982);
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(0, recordsArrayMap983);
        Map<Utf8, List<IndexedRecord>> recordsMapArray1001 = null;
        long chunkLen1002 = (decoder.readMapStart());
        if (chunkLen1002 > 0) {
            Map<Utf8, List<IndexedRecord>> recordsMapArrayReuse1003 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(1) instanceof Map) {
                recordsMapArrayReuse1003 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(1));
            }
            if (recordsMapArrayReuse1003 != (null)) {
                recordsMapArrayReuse1003 .clear();
                recordsMapArray1001 = recordsMapArrayReuse1003;
            } else {
                recordsMapArray1001 = new HashMap<Utf8, List<IndexedRecord>>();
            }
            do {
                for (int counter1004 = 0; (counter1004 <chunkLen1002); counter1004 ++) {
                    Utf8 key1005 = (decoder.readString(null));
                    List<IndexedRecord> recordsMapArrayValue1007 = null;
                    long chunkLen1008 = (decoder.readArrayStart());
                    if (chunkLen1008 > 0) {
                        List<IndexedRecord> recordsMapArrayValueReuse1009 = null;
                        if (null instanceof List) {
                            recordsMapArrayValueReuse1009 = ((List) null);
                        }
                        if (recordsMapArrayValueReuse1009 != (null)) {
                            recordsMapArrayValueReuse1009 .clear();
                            recordsMapArrayValue1007 = recordsMapArrayValueReuse1009;
                        } else {
                            recordsMapArrayValue1007 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen1008), recordsMapArrayMapValueSchema1006);
                        }
                        do {
                            for (int counter1010 = 0; (counter1010 <chunkLen1008); counter1010 ++) {
                                Object recordsMapArrayValueArrayElementReuseVar1012 = null;
                                if (null instanceof GenericArray) {
                                    recordsMapArrayValueArrayElementReuseVar1012 = ((GenericArray) null).peek();
                                }
                                int unionIndex1013 = (decoder.readIndex());
                                if (unionIndex1013 == 0) {
                                    decoder.readNull();
                                }
                                if (unionIndex1013 == 1) {
                                    recordsMapArrayValue1007 .add(deserializesubRecord997(recordsMapArrayValueArrayElementReuseVar1012, (decoder)));
                                }
                            }
                            chunkLen1008 = (decoder.arrayNext());
                        } while (chunkLen1008 > 0);
                    } else {
                        recordsMapArrayValue1007 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsMapArrayMapValueSchema1006);
                    }
                    recordsMapArray1001 .put(key1005, recordsMapArrayValue1007);
                }
                chunkLen1002 = (decoder.mapNext());
            } while (chunkLen1002 > 0);
        } else {
            recordsMapArray1001 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(1, recordsMapArray1001);
        int unionIndex1015 = (decoder.readIndex());
        if (unionIndex1015 == 0) {
            decoder.readNull();
        }
        if (unionIndex1015 == 1) {
            List<Map<Utf8, IndexedRecord>> recordsArrayMapUnionOption1016 = null;
            long chunkLen1017 = (decoder.readArrayStart());
            if (chunkLen1017 > 0) {
                List<Map<Utf8, IndexedRecord>> recordsArrayMapUnionOptionReuse1018 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2) instanceof List) {
                    recordsArrayMapUnionOptionReuse1018 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2));
                }
                if (recordsArrayMapUnionOptionReuse1018 != (null)) {
                    recordsArrayMapUnionOptionReuse1018 .clear();
                    recordsArrayMapUnionOption1016 = recordsArrayMapUnionOptionReuse1018;
                } else {
                    recordsArrayMapUnionOption1016 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen1017), recordsArrayMap982);
                }
                do {
                    for (int counter1019 = 0; (counter1019 <chunkLen1017); counter1019 ++) {
                        Object recordsArrayMapUnionOptionArrayElementReuseVar1021 = null;
                        if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2) instanceof GenericArray) {
                            recordsArrayMapUnionOptionArrayElementReuseVar1021 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2)).peek();
                        }
                        Map<Utf8, IndexedRecord> recordsArrayMapUnionOptionElem1022 = null;
                        long chunkLen1023 = (decoder.readMapStart());
                        if (chunkLen1023 > 0) {
                            Map<Utf8, IndexedRecord> recordsArrayMapUnionOptionElemReuse1024 = null;
                            if (recordsArrayMapUnionOptionArrayElementReuseVar1021 instanceof Map) {
                                recordsArrayMapUnionOptionElemReuse1024 = ((Map) recordsArrayMapUnionOptionArrayElementReuseVar1021);
                            }
                            if (recordsArrayMapUnionOptionElemReuse1024 != (null)) {
                                recordsArrayMapUnionOptionElemReuse1024 .clear();
                                recordsArrayMapUnionOptionElem1022 = recordsArrayMapUnionOptionElemReuse1024;
                            } else {
                                recordsArrayMapUnionOptionElem1022 = new HashMap<Utf8, IndexedRecord>();
                            }
                            do {
                                for (int counter1025 = 0; (counter1025 <chunkLen1023); counter1025 ++) {
                                    Utf8 key1026 = (decoder.readString(null));
                                    int unionIndex1028 = (decoder.readIndex());
                                    if (unionIndex1028 == 0) {
                                        decoder.readNull();
                                    }
                                    if (unionIndex1028 == 1) {
                                        recordsArrayMapUnionOptionElem1022 .put(key1026, deserializesubRecord997(null, (decoder)));
                                    }
                                }
                                chunkLen1023 = (decoder.mapNext());
                            } while (chunkLen1023 > 0);
                        } else {
                            recordsArrayMapUnionOptionElem1022 = Collections.emptyMap();
                        }
                        recordsArrayMapUnionOption1016 .add(recordsArrayMapUnionOptionElem1022);
                    }
                    chunkLen1017 = (decoder.arrayNext());
                } while (chunkLen1017 > 0);
            } else {
                recordsArrayMapUnionOption1016 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(0, recordsArrayMap982);
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(2, recordsArrayMapUnionOption1016);
        }
        int unionIndex1030 = (decoder.readIndex());
        if (unionIndex1030 == 0) {
            decoder.readNull();
        }
        if (unionIndex1030 == 1) {
            Map<Utf8, List<IndexedRecord>> recordsMapArrayUnionOption1032 = null;
            long chunkLen1033 = (decoder.readMapStart());
            if (chunkLen1033 > 0) {
                Map<Utf8, List<IndexedRecord>> recordsMapArrayUnionOptionReuse1034 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(3) instanceof Map) {
                    recordsMapArrayUnionOptionReuse1034 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(3));
                }
                if (recordsMapArrayUnionOptionReuse1034 != (null)) {
                    recordsMapArrayUnionOptionReuse1034 .clear();
                    recordsMapArrayUnionOption1032 = recordsMapArrayUnionOptionReuse1034;
                } else {
                    recordsMapArrayUnionOption1032 = new HashMap<Utf8, List<IndexedRecord>>();
                }
                do {
                    for (int counter1035 = 0; (counter1035 <chunkLen1033); counter1035 ++) {
                        Utf8 key1036 = (decoder.readString(null));
                        List<IndexedRecord> recordsMapArrayUnionOptionValue1037 = null;
                        long chunkLen1038 = (decoder.readArrayStart());
                        if (chunkLen1038 > 0) {
                            List<IndexedRecord> recordsMapArrayUnionOptionValueReuse1039 = null;
                            if (null instanceof List) {
                                recordsMapArrayUnionOptionValueReuse1039 = ((List) null);
                            }
                            if (recordsMapArrayUnionOptionValueReuse1039 != (null)) {
                                recordsMapArrayUnionOptionValueReuse1039 .clear();
                                recordsMapArrayUnionOptionValue1037 = recordsMapArrayUnionOptionValueReuse1039;
                            } else {
                                recordsMapArrayUnionOptionValue1037 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen1038), recordsMapArrayMapValueSchema1006);
                            }
                            do {
                                for (int counter1040 = 0; (counter1040 <chunkLen1038); counter1040 ++) {
                                    Object recordsMapArrayUnionOptionValueArrayElementReuseVar1042 = null;
                                    if (null instanceof GenericArray) {
                                        recordsMapArrayUnionOptionValueArrayElementReuseVar1042 = ((GenericArray) null).peek();
                                    }
                                    int unionIndex1043 = (decoder.readIndex());
                                    if (unionIndex1043 == 0) {
                                        decoder.readNull();
                                    }
                                    if (unionIndex1043 == 1) {
                                        recordsMapArrayUnionOptionValue1037 .add(deserializesubRecord997(recordsMapArrayUnionOptionValueArrayElementReuseVar1042, (decoder)));
                                    }
                                }
                                chunkLen1038 = (decoder.arrayNext());
                            } while (chunkLen1038 > 0);
                        } else {
                            recordsMapArrayUnionOptionValue1037 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsMapArrayMapValueSchema1006);
                        }
                        recordsMapArrayUnionOption1032 .put(key1036, recordsMapArrayUnionOptionValue1037);
                    }
                    chunkLen1033 = (decoder.mapNext());
                } while (chunkLen1033 > 0);
            } else {
                recordsMapArrayUnionOption1032 = Collections.emptyMap();
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(3, recordsMapArrayUnionOption1032);
        }
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField;
    }

    public IndexedRecord deserializesubRecord997(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordsArrayMapElemValueOptionSchema996)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordsArrayMapElemValueOptionSchema996);
        }
        int unionIndex999 = (decoder.readIndex());
        if (unionIndex999 == 0) {
            decoder.readNull();
        }
        if (unionIndex999 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
