
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

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
    private final Schema recordsArrayMap836;
    private final Schema recordsArrayMapArrayElemSchema841;
    private final Schema recordsArrayMapElemMapValueSchema848;
    private final Schema recordsArrayMapElemValueOptionSchema850;
    private final Schema subField852;
    private final Schema recordsMapArray854;
    private final Schema recordsMapArrayMapValueSchema860;
    private final Schema recordsMapArrayValueArrayElemSchema865;
    private final Schema recordsArrayMapUnion868;
    private final Schema recordsArrayMapUnionOptionArrayElemSchema874;
    private final Schema recordsArrayMapUnionOptionElemMapValueSchema881;
    private final Schema recordsMapArrayUnion883;
    private final Schema recordsMapArrayUnionOptionSchema885;
    private final Schema recordsMapArrayUnionOptionValueArrayElemSchema895;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField_GenericDeserializer_1368792156564876103_1368792156564876103(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordsArrayMap836 = readerSchema.getField("recordsArrayMap").schema();
        this.recordsArrayMapArrayElemSchema841 = recordsArrayMap836 .getElementType();
        this.recordsArrayMapElemMapValueSchema848 = recordsArrayMapArrayElemSchema841 .getValueType();
        this.recordsArrayMapElemValueOptionSchema850 = recordsArrayMapElemMapValueSchema848 .getTypes().get(1);
        this.subField852 = recordsArrayMapElemValueOptionSchema850 .getField("subField").schema();
        this.recordsMapArray854 = readerSchema.getField("recordsMapArray").schema();
        this.recordsMapArrayMapValueSchema860 = recordsMapArray854 .getValueType();
        this.recordsMapArrayValueArrayElemSchema865 = recordsMapArrayMapValueSchema860 .getElementType();
        this.recordsArrayMapUnion868 = readerSchema.getField("recordsArrayMapUnion").schema();
        this.recordsArrayMapUnionOptionArrayElemSchema874 = recordsArrayMap836 .getElementType();
        this.recordsArrayMapUnionOptionElemMapValueSchema881 = recordsArrayMapUnionOptionArrayElemSchema874 .getValueType();
        this.recordsMapArrayUnion883 = readerSchema.getField("recordsMapArrayUnion").schema();
        this.recordsMapArrayUnionOptionSchema885 = recordsMapArrayUnion883 .getTypes().get(1);
        this.recordsMapArrayUnionOptionValueArrayElemSchema895 = recordsMapArrayMapValueSchema860 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField835((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField835(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<Map<Utf8, IndexedRecord>> recordsArrayMap837 = null;
        long chunkLen838 = (decoder.readArrayStart());
        if (chunkLen838 > 0) {
            List<Map<Utf8, IndexedRecord>> recordsArrayMapReuse839 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0) instanceof List) {
                recordsArrayMapReuse839 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0));
            }
            if (recordsArrayMapReuse839 != (null)) {
                recordsArrayMapReuse839 .clear();
                recordsArrayMap837 = recordsArrayMapReuse839;
            } else {
                recordsArrayMap837 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen838), recordsArrayMap836);
            }
            do {
                for (int counter840 = 0; (counter840 <chunkLen838); counter840 ++) {
                    Object recordsArrayMapArrayElementReuseVar842 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0) instanceof GenericArray) {
                        recordsArrayMapArrayElementReuseVar842 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(0)).peek();
                    }
                    Map<Utf8, IndexedRecord> recordsArrayMapElem843 = null;
                    long chunkLen844 = (decoder.readMapStart());
                    if (chunkLen844 > 0) {
                        Map<Utf8, IndexedRecord> recordsArrayMapElemReuse845 = null;
                        if (recordsArrayMapArrayElementReuseVar842 instanceof Map) {
                            recordsArrayMapElemReuse845 = ((Map) recordsArrayMapArrayElementReuseVar842);
                        }
                        if (recordsArrayMapElemReuse845 != (null)) {
                            recordsArrayMapElemReuse845 .clear();
                            recordsArrayMapElem843 = recordsArrayMapElemReuse845;
                        } else {
                            recordsArrayMapElem843 = new HashMap<Utf8, IndexedRecord>();
                        }
                        do {
                            for (int counter846 = 0; (counter846 <chunkLen844); counter846 ++) {
                                Utf8 key847 = (decoder.readString(null));
                                int unionIndex849 = (decoder.readIndex());
                                if (unionIndex849 == 0) {
                                    decoder.readNull();
                                }
                                if (unionIndex849 == 1) {
                                    recordsArrayMapElem843 .put(key847, deserializesubRecord851(null, (decoder)));
                                }
                            }
                            chunkLen844 = (decoder.mapNext());
                        } while (chunkLen844 > 0);
                    } else {
                        recordsArrayMapElem843 = Collections.emptyMap();
                    }
                    recordsArrayMap837 .add(recordsArrayMapElem843);
                }
                chunkLen838 = (decoder.arrayNext());
            } while (chunkLen838 > 0);
        } else {
            recordsArrayMap837 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(0, recordsArrayMap836);
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(0, recordsArrayMap837);
        Map<Utf8, List<IndexedRecord>> recordsMapArray855 = null;
        long chunkLen856 = (decoder.readMapStart());
        if (chunkLen856 > 0) {
            Map<Utf8, List<IndexedRecord>> recordsMapArrayReuse857 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(1) instanceof Map) {
                recordsMapArrayReuse857 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(1));
            }
            if (recordsMapArrayReuse857 != (null)) {
                recordsMapArrayReuse857 .clear();
                recordsMapArray855 = recordsMapArrayReuse857;
            } else {
                recordsMapArray855 = new HashMap<Utf8, List<IndexedRecord>>();
            }
            do {
                for (int counter858 = 0; (counter858 <chunkLen856); counter858 ++) {
                    Utf8 key859 = (decoder.readString(null));
                    List<IndexedRecord> recordsMapArrayValue861 = null;
                    long chunkLen862 = (decoder.readArrayStart());
                    if (chunkLen862 > 0) {
                        List<IndexedRecord> recordsMapArrayValueReuse863 = null;
                        if (null instanceof List) {
                            recordsMapArrayValueReuse863 = ((List) null);
                        }
                        if (recordsMapArrayValueReuse863 != (null)) {
                            recordsMapArrayValueReuse863 .clear();
                            recordsMapArrayValue861 = recordsMapArrayValueReuse863;
                        } else {
                            recordsMapArrayValue861 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen862), recordsMapArrayMapValueSchema860);
                        }
                        do {
                            for (int counter864 = 0; (counter864 <chunkLen862); counter864 ++) {
                                Object recordsMapArrayValueArrayElementReuseVar866 = null;
                                if (null instanceof GenericArray) {
                                    recordsMapArrayValueArrayElementReuseVar866 = ((GenericArray) null).peek();
                                }
                                int unionIndex867 = (decoder.readIndex());
                                if (unionIndex867 == 0) {
                                    decoder.readNull();
                                }
                                if (unionIndex867 == 1) {
                                    recordsMapArrayValue861 .add(deserializesubRecord851(recordsMapArrayValueArrayElementReuseVar866, (decoder)));
                                }
                            }
                            chunkLen862 = (decoder.arrayNext());
                        } while (chunkLen862 > 0);
                    } else {
                        recordsMapArrayValue861 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsMapArrayMapValueSchema860);
                    }
                    recordsMapArray855 .put(key859, recordsMapArrayValue861);
                }
                chunkLen856 = (decoder.mapNext());
            } while (chunkLen856 > 0);
        } else {
            recordsMapArray855 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(1, recordsMapArray855);
        int unionIndex869 = (decoder.readIndex());
        if (unionIndex869 == 0) {
            decoder.readNull();
        }
        if (unionIndex869 == 1) {
            List<Map<Utf8, IndexedRecord>> recordsArrayMapUnionOption870 = null;
            long chunkLen871 = (decoder.readArrayStart());
            if (chunkLen871 > 0) {
                List<Map<Utf8, IndexedRecord>> recordsArrayMapUnionOptionReuse872 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2) instanceof List) {
                    recordsArrayMapUnionOptionReuse872 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2));
                }
                if (recordsArrayMapUnionOptionReuse872 != (null)) {
                    recordsArrayMapUnionOptionReuse872 .clear();
                    recordsArrayMapUnionOption870 = recordsArrayMapUnionOptionReuse872;
                } else {
                    recordsArrayMapUnionOption870 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(((int) chunkLen871), recordsArrayMap836);
                }
                do {
                    for (int counter873 = 0; (counter873 <chunkLen871); counter873 ++) {
                        Object recordsArrayMapUnionOptionArrayElementReuseVar875 = null;
                        if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2) instanceof GenericArray) {
                            recordsArrayMapUnionOptionArrayElementReuseVar875 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(2)).peek();
                        }
                        Map<Utf8, IndexedRecord> recordsArrayMapUnionOptionElem876 = null;
                        long chunkLen877 = (decoder.readMapStart());
                        if (chunkLen877 > 0) {
                            Map<Utf8, IndexedRecord> recordsArrayMapUnionOptionElemReuse878 = null;
                            if (recordsArrayMapUnionOptionArrayElementReuseVar875 instanceof Map) {
                                recordsArrayMapUnionOptionElemReuse878 = ((Map) recordsArrayMapUnionOptionArrayElementReuseVar875);
                            }
                            if (recordsArrayMapUnionOptionElemReuse878 != (null)) {
                                recordsArrayMapUnionOptionElemReuse878 .clear();
                                recordsArrayMapUnionOptionElem876 = recordsArrayMapUnionOptionElemReuse878;
                            } else {
                                recordsArrayMapUnionOptionElem876 = new HashMap<Utf8, IndexedRecord>();
                            }
                            do {
                                for (int counter879 = 0; (counter879 <chunkLen877); counter879 ++) {
                                    Utf8 key880 = (decoder.readString(null));
                                    int unionIndex882 = (decoder.readIndex());
                                    if (unionIndex882 == 0) {
                                        decoder.readNull();
                                    }
                                    if (unionIndex882 == 1) {
                                        recordsArrayMapUnionOptionElem876 .put(key880, deserializesubRecord851(null, (decoder)));
                                    }
                                }
                                chunkLen877 = (decoder.mapNext());
                            } while (chunkLen877 > 0);
                        } else {
                            recordsArrayMapUnionOptionElem876 = Collections.emptyMap();
                        }
                        recordsArrayMapUnionOption870 .add(recordsArrayMapUnionOptionElem876);
                    }
                    chunkLen871 = (decoder.arrayNext());
                } while (chunkLen871 > 0);
            } else {
                recordsArrayMapUnionOption870 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, IndexedRecord>>(0, recordsArrayMap836);
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(2, recordsArrayMapUnionOption870);
        }
        int unionIndex884 = (decoder.readIndex());
        if (unionIndex884 == 0) {
            decoder.readNull();
        }
        if (unionIndex884 == 1) {
            Map<Utf8, List<IndexedRecord>> recordsMapArrayUnionOption886 = null;
            long chunkLen887 = (decoder.readMapStart());
            if (chunkLen887 > 0) {
                Map<Utf8, List<IndexedRecord>> recordsMapArrayUnionOptionReuse888 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(3) instanceof Map) {
                    recordsMapArrayUnionOptionReuse888 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.get(3));
                }
                if (recordsMapArrayUnionOptionReuse888 != (null)) {
                    recordsMapArrayUnionOptionReuse888 .clear();
                    recordsMapArrayUnionOption886 = recordsMapArrayUnionOptionReuse888;
                } else {
                    recordsMapArrayUnionOption886 = new HashMap<Utf8, List<IndexedRecord>>();
                }
                do {
                    for (int counter889 = 0; (counter889 <chunkLen887); counter889 ++) {
                        Utf8 key890 = (decoder.readString(null));
                        List<IndexedRecord> recordsMapArrayUnionOptionValue891 = null;
                        long chunkLen892 = (decoder.readArrayStart());
                        if (chunkLen892 > 0) {
                            List<IndexedRecord> recordsMapArrayUnionOptionValueReuse893 = null;
                            if (null instanceof List) {
                                recordsMapArrayUnionOptionValueReuse893 = ((List) null);
                            }
                            if (recordsMapArrayUnionOptionValueReuse893 != (null)) {
                                recordsMapArrayUnionOptionValueReuse893 .clear();
                                recordsMapArrayUnionOptionValue891 = recordsMapArrayUnionOptionValueReuse893;
                            } else {
                                recordsMapArrayUnionOptionValue891 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen892), recordsMapArrayMapValueSchema860);
                            }
                            do {
                                for (int counter894 = 0; (counter894 <chunkLen892); counter894 ++) {
                                    Object recordsMapArrayUnionOptionValueArrayElementReuseVar896 = null;
                                    if (null instanceof GenericArray) {
                                        recordsMapArrayUnionOptionValueArrayElementReuseVar896 = ((GenericArray) null).peek();
                                    }
                                    int unionIndex897 = (decoder.readIndex());
                                    if (unionIndex897 == 0) {
                                        decoder.readNull();
                                    }
                                    if (unionIndex897 == 1) {
                                        recordsMapArrayUnionOptionValue891 .add(deserializesubRecord851(recordsMapArrayUnionOptionValueArrayElementReuseVar896, (decoder)));
                                    }
                                }
                                chunkLen892 = (decoder.arrayNext());
                            } while (chunkLen892 > 0);
                        } else {
                            recordsMapArrayUnionOptionValue891 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsMapArrayMapValueSchema860);
                        }
                        recordsMapArrayUnionOption886 .put(key890, recordsMapArrayUnionOptionValue891);
                    }
                    chunkLen887 = (decoder.mapNext());
                } while (chunkLen887 > 0);
            } else {
                recordsMapArrayUnionOption886 = Collections.emptyMap();
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField.put(3, recordsMapArrayUnionOption886);
        }
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordComplexCollectionsField;
    }

    public IndexedRecord deserializesubRecord851(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordsArrayMapElemValueOptionSchema850)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordsArrayMapElemValueOptionSchema850);
        }
        int unionIndex853 = (decoder.readIndex());
        if (unionIndex853 == 0) {
            decoder.readNull();
        }
        if (unionIndex853 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
