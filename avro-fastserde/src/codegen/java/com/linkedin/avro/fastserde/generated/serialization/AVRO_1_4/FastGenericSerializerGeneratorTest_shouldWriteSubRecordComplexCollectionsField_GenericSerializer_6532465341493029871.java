
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordComplexCollectionsField_GenericSerializer_6532465341493029871
    implements FastSerializer<IndexedRecord>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordComplexCollectionsField130(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordComplexCollectionsField130(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<Map<CharSequence, IndexedRecord>> recordsArrayMap131 = ((List<Map<CharSequence, IndexedRecord>> ) data.get(0));
        (encoder).writeArrayStart();
        if ((recordsArrayMap131 == null)||recordsArrayMap131 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsArrayMap131 .size());
            for (int counter132 = 0; (counter132 <((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMap131).size()); counter132 ++) {
                (encoder).startItem();
                Map<CharSequence, IndexedRecord> map133 = null;
                map133 = ((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMap131).get(counter132);
                (encoder).writeMapStart();
                if ((map133 == null)||map133 .isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(map133 .size());
                    for (CharSequence key134 : ((Map<CharSequence, IndexedRecord> ) map133).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key134);
                        IndexedRecord union135 = null;
                        union135 = ((Map<CharSequence, IndexedRecord> ) map133).get(key134);
                        if (union135 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union135 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union135).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord136(((IndexedRecord) union135), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeMapEnd();
            }
        }
        (encoder).writeArrayEnd();
        Map<CharSequence, List<IndexedRecord>> recordsMapArray138 = ((Map<CharSequence, List<IndexedRecord>> ) data.get(1));
        (encoder).writeMapStart();
        if ((recordsMapArray138 == null)||recordsMapArray138 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsMapArray138 .size());
            for (CharSequence key139 : ((Map<CharSequence, List<IndexedRecord>> ) recordsMapArray138).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key139);
                List<IndexedRecord> array140 = null;
                array140 = ((Map<CharSequence, List<IndexedRecord>> ) recordsMapArray138).get(key139);
                (encoder).writeArrayStart();
                if ((array140 == null)||array140 .isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(array140 .size());
                    for (int counter141 = 0; (counter141 <((List<IndexedRecord> ) array140).size()); counter141 ++) {
                        (encoder).startItem();
                        IndexedRecord union142 = null;
                        union142 = ((List<IndexedRecord> ) array140).get(counter141);
                        if (union142 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union142 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union142).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord136(((IndexedRecord) union142), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        (encoder).writeMapEnd();
        List<Map<CharSequence, IndexedRecord>> recordsArrayMapUnion143 = ((List<Map<CharSequence, IndexedRecord>> ) data.get(2));
        if (recordsArrayMapUnion143 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsArrayMapUnion143 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion143) == null)||((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion143).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion143).size());
                    for (int counter144 = 0; (counter144 <((List<Map<CharSequence, IndexedRecord>> )((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion143)).size()); counter144 ++) {
                        (encoder).startItem();
                        Map<CharSequence, IndexedRecord> map145 = null;
                        map145 = ((List<Map<CharSequence, IndexedRecord>> )((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion143)).get(counter144);
                        (encoder).writeMapStart();
                        if ((map145 == null)||map145 .isEmpty()) {
                            (encoder).setItemCount(0);
                        } else {
                            (encoder).setItemCount(map145 .size());
                            for (CharSequence key146 : ((Map<CharSequence, IndexedRecord> ) map145).keySet()) {
                                (encoder).startItem();
                                (encoder).writeString(key146);
                                IndexedRecord union147 = null;
                                union147 = ((Map<CharSequence, IndexedRecord> ) map145).get(key146);
                                if (union147 == null) {
                                    (encoder).writeIndex(0);
                                    (encoder).writeNull();
                                } else {
                                    if ((union147 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union147).getSchema().getFullName())) {
                                        (encoder).writeIndex(1);
                                        serializesubRecord136(((IndexedRecord) union147), (encoder));
                                    }
                                }
                            }
                        }
                        (encoder).writeMapEnd();
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        Map<CharSequence, List<IndexedRecord>> recordsMapArrayUnion148 = ((Map<CharSequence, List<IndexedRecord>> ) data.get(3));
        if (recordsMapArrayUnion148 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsMapArrayUnion148 instanceof Map) {
                (encoder).writeIndex(1);
                (encoder).writeMapStart();
                if ((((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion148) == null)||((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion148).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion148).size());
                    for (CharSequence key149 : ((Map<CharSequence, List<IndexedRecord>> )((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion148)).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key149);
                        List<IndexedRecord> array150 = null;
                        array150 = ((Map<CharSequence, List<IndexedRecord>> )((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion148)).get(key149);
                        (encoder).writeArrayStart();
                        if ((array150 == null)||array150 .isEmpty()) {
                            (encoder).setItemCount(0);
                        } else {
                            (encoder).setItemCount(array150 .size());
                            for (int counter151 = 0; (counter151 <((List<IndexedRecord> ) array150).size()); counter151 ++) {
                                (encoder).startItem();
                                IndexedRecord union152 = null;
                                union152 = ((List<IndexedRecord> ) array150).get(counter151);
                                if (union152 == null) {
                                    (encoder).writeIndex(0);
                                    (encoder).writeNull();
                                } else {
                                    if ((union152 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union152).getSchema().getFullName())) {
                                        (encoder).writeIndex(1);
                                        serializesubRecord136(((IndexedRecord) union152), (encoder));
                                    }
                                }
                            }
                        }
                        (encoder).writeArrayEnd();
                    }
                }
                (encoder).writeMapEnd();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializesubRecord136(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField137 = ((CharSequence) data.get(0));
        if (subField137 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField137 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField137 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField137));
                } else {
                    (encoder).writeString(subField137 .toString());
                }
            }
        }
    }

}
