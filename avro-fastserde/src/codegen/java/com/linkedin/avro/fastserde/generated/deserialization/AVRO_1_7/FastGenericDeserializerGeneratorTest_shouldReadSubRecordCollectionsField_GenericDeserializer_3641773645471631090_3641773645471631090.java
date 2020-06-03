
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

public class FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField_GenericDeserializer_3641773645471631090_3641773645471631090
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema recordsArray799;
    private final Schema recordsArrayArrayElemSchema804;
    private final Schema subField807;
    private final Schema recordsMap809;
    private final Schema recordsArrayUnion815;
    private final Schema recordsArrayUnionOptionSchema817;
    private final Schema recordsArrayUnionOptionArrayElemSchema822;
    private final Schema recordsMapUnion825;
    private final Schema recordsMapUnionOptionSchema827;
    private final Schema recordsMapUnionOptionMapValueSchema833;

    public FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField_GenericDeserializer_3641773645471631090_3641773645471631090(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.recordsArray799 = readerSchema.getField("recordsArray").schema();
        this.recordsArrayArrayElemSchema804 = recordsArray799 .getElementType();
        this.subField807 = recordsArrayArrayElemSchema804 .getField("subField").schema();
        this.recordsMap809 = readerSchema.getField("recordsMap").schema();
        this.recordsArrayUnion815 = readerSchema.getField("recordsArrayUnion").schema();
        this.recordsArrayUnionOptionSchema817 = recordsArrayUnion815 .getTypes().get(1);
        this.recordsArrayUnionOptionArrayElemSchema822 = recordsArrayUnionOptionSchema817 .getElementType();
        this.recordsMapUnion825 = readerSchema.getField("recordsMapUnion").schema();
        this.recordsMapUnionOptionSchema827 = recordsMapUnion825 .getTypes().get(1);
        this.recordsMapUnionOptionMapValueSchema833 = recordsMapUnionOptionSchema827 .getValueType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField798((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField798(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<IndexedRecord> recordsArray800 = null;
        long chunkLen801 = (decoder.readArrayStart());
        if (chunkLen801 > 0) {
            List<IndexedRecord> recordsArrayReuse802 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0) instanceof List) {
                recordsArrayReuse802 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0));
            }
            if (recordsArrayReuse802 != (null)) {
                recordsArrayReuse802 .clear();
                recordsArray800 = recordsArrayReuse802;
            } else {
                recordsArray800 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen801), recordsArray799);
            }
            do {
                for (int counter803 = 0; (counter803 <chunkLen801); counter803 ++) {
                    Object recordsArrayArrayElementReuseVar805 = null;
                    if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0) instanceof GenericArray) {
                        recordsArrayArrayElementReuseVar805 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(0)).peek();
                    }
                    recordsArray800 .add(deserializesubRecord806(recordsArrayArrayElementReuseVar805, (decoder)));
                }
                chunkLen801 = (decoder.arrayNext());
            } while (chunkLen801 > 0);
        } else {
            recordsArray800 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsArray799);
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(0, recordsArray800);
        Map<Utf8, IndexedRecord> recordsMap810 = null;
        long chunkLen811 = (decoder.readMapStart());
        if (chunkLen811 > 0) {
            Map<Utf8, IndexedRecord> recordsMapReuse812 = null;
            if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(1) instanceof Map) {
                recordsMapReuse812 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(1));
            }
            if (recordsMapReuse812 != (null)) {
                recordsMapReuse812 .clear();
                recordsMap810 = recordsMapReuse812;
            } else {
                recordsMap810 = new HashMap<Utf8, IndexedRecord>();
            }
            do {
                for (int counter813 = 0; (counter813 <chunkLen811); counter813 ++) {
                    Utf8 key814 = (decoder.readString(null));
                    recordsMap810 .put(key814, deserializesubRecord806(null, (decoder)));
                }
                chunkLen811 = (decoder.mapNext());
            } while (chunkLen811 > 0);
        } else {
            recordsMap810 = Collections.emptyMap();
        }
        FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(1, recordsMap810);
        int unionIndex816 = (decoder.readIndex());
        if (unionIndex816 == 0) {
            decoder.readNull();
        }
        if (unionIndex816 == 1) {
            List<IndexedRecord> recordsArrayUnionOption818 = null;
            long chunkLen819 = (decoder.readArrayStart());
            if (chunkLen819 > 0) {
                List<IndexedRecord> recordsArrayUnionOptionReuse820 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2) instanceof List) {
                    recordsArrayUnionOptionReuse820 = ((List) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2));
                }
                if (recordsArrayUnionOptionReuse820 != (null)) {
                    recordsArrayUnionOptionReuse820 .clear();
                    recordsArrayUnionOption818 = recordsArrayUnionOptionReuse820;
                } else {
                    recordsArrayUnionOption818 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(((int) chunkLen819), recordsArrayUnionOptionSchema817);
                }
                do {
                    for (int counter821 = 0; (counter821 <chunkLen819); counter821 ++) {
                        Object recordsArrayUnionOptionArrayElementReuseVar823 = null;
                        if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2) instanceof GenericArray) {
                            recordsArrayUnionOptionArrayElementReuseVar823 = ((GenericArray) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(2)).peek();
                        }
                        int unionIndex824 = (decoder.readIndex());
                        if (unionIndex824 == 0) {
                            decoder.readNull();
                        }
                        if (unionIndex824 == 1) {
                            recordsArrayUnionOption818 .add(deserializesubRecord806(recordsArrayUnionOptionArrayElementReuseVar823, (decoder)));
                        }
                    }
                    chunkLen819 = (decoder.arrayNext());
                } while (chunkLen819 > 0);
            } else {
                recordsArrayUnionOption818 = new org.apache.avro.generic.GenericData.Array<IndexedRecord>(0, recordsArrayUnionOptionSchema817);
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(2, recordsArrayUnionOption818);
        }
        int unionIndex826 = (decoder.readIndex());
        if (unionIndex826 == 0) {
            decoder.readNull();
        }
        if (unionIndex826 == 1) {
            Map<Utf8, IndexedRecord> recordsMapUnionOption828 = null;
            long chunkLen829 = (decoder.readMapStart());
            if (chunkLen829 > 0) {
                Map<Utf8, IndexedRecord> recordsMapUnionOptionReuse830 = null;
                if (FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(3) instanceof Map) {
                    recordsMapUnionOptionReuse830 = ((Map) FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.get(3));
                }
                if (recordsMapUnionOptionReuse830 != (null)) {
                    recordsMapUnionOptionReuse830 .clear();
                    recordsMapUnionOption828 = recordsMapUnionOptionReuse830;
                } else {
                    recordsMapUnionOption828 = new HashMap<Utf8, IndexedRecord>();
                }
                do {
                    for (int counter831 = 0; (counter831 <chunkLen829); counter831 ++) {
                        Utf8 key832 = (decoder.readString(null));
                        int unionIndex834 = (decoder.readIndex());
                        if (unionIndex834 == 0) {
                            decoder.readNull();
                        }
                        if (unionIndex834 == 1) {
                            recordsMapUnionOption828 .put(key832, deserializesubRecord806(null, (decoder)));
                        }
                    }
                    chunkLen829 = (decoder.mapNext());
                } while (chunkLen829 > 0);
            } else {
                recordsMapUnionOption828 = Collections.emptyMap();
            }
            FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField.put(3, recordsMapUnionOption828);
        }
        return FastGenericDeserializerGeneratorTest_shouldReadSubRecordCollectionsField;
    }

    public IndexedRecord deserializesubRecord806(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == recordsArrayArrayElemSchema804)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(recordsArrayArrayElemSchema804);
        }
        int unionIndex808 = (decoder.readIndex());
        if (unionIndex808 == 0) {
            decoder.readNull();
        }
        if (unionIndex808 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
