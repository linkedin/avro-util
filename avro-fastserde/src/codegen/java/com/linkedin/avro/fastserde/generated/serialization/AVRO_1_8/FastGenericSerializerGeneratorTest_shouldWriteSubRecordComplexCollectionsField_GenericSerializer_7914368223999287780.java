
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordComplexCollectionsField_GenericSerializer_7914368223999287780
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordComplexCollectionsField129(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordComplexCollectionsField129(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<Map<CharSequence, IndexedRecord>> recordsArrayMap130 = ((List<Map<CharSequence, IndexedRecord>> ) data.get(0));
        (encoder).writeArrayStart();
        if ((recordsArrayMap130 == null)||recordsArrayMap130 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsArrayMap130 .size());
            for (int counter131 = 0; (counter131 <((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMap130).size()); counter131 ++) {
                (encoder).startItem();
                Map<CharSequence, IndexedRecord> map132 = null;
                map132 = ((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMap130).get(counter131);
                (encoder).writeMapStart();
                if ((map132 == null)||map132 .isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(map132 .size());
                    for (CharSequence key133 : ((Map<CharSequence, IndexedRecord> ) map132).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key133);
                        IndexedRecord union134 = null;
                        union134 = ((Map<CharSequence, IndexedRecord> ) map132).get(key133);
                        if (union134 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union134 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union134).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord135(((IndexedRecord) union134), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeMapEnd();
            }
        }
        (encoder).writeArrayEnd();
        Map<CharSequence, List<IndexedRecord>> recordsMapArray137 = ((Map<CharSequence, List<IndexedRecord>> ) data.get(1));
        (encoder).writeMapStart();
        if ((recordsMapArray137 == null)||recordsMapArray137 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsMapArray137 .size());
            for (CharSequence key138 : ((Map<CharSequence, List<IndexedRecord>> ) recordsMapArray137).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key138);
                List<IndexedRecord> array139 = null;
                array139 = ((Map<CharSequence, List<IndexedRecord>> ) recordsMapArray137).get(key138);
                (encoder).writeArrayStart();
                if ((array139 == null)||array139 .isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(array139 .size());
                    for (int counter140 = 0; (counter140 <((List<IndexedRecord> ) array139).size()); counter140 ++) {
                        (encoder).startItem();
                        IndexedRecord union141 = null;
                        union141 = ((List<IndexedRecord> ) array139).get(counter140);
                        if (union141 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union141 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union141).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord135(((IndexedRecord) union141), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        (encoder).writeMapEnd();
        List<Map<CharSequence, IndexedRecord>> recordsArrayMapUnion142 = ((List<Map<CharSequence, IndexedRecord>> ) data.get(2));
        if (recordsArrayMapUnion142 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsArrayMapUnion142 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion142) == null)||((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion142).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion142).size());
                    for (int counter143 = 0; (counter143 <((List<Map<CharSequence, IndexedRecord>> )((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion142)).size()); counter143 ++) {
                        (encoder).startItem();
                        Map<CharSequence, IndexedRecord> map144 = null;
                        map144 = ((List<Map<CharSequence, IndexedRecord>> )((List<Map<CharSequence, IndexedRecord>> ) recordsArrayMapUnion142)).get(counter143);
                        (encoder).writeMapStart();
                        if ((map144 == null)||map144 .isEmpty()) {
                            (encoder).setItemCount(0);
                        } else {
                            (encoder).setItemCount(map144 .size());
                            for (CharSequence key145 : ((Map<CharSequence, IndexedRecord> ) map144).keySet()) {
                                (encoder).startItem();
                                (encoder).writeString(key145);
                                IndexedRecord union146 = null;
                                union146 = ((Map<CharSequence, IndexedRecord> ) map144).get(key145);
                                if (union146 == null) {
                                    (encoder).writeIndex(0);
                                    (encoder).writeNull();
                                } else {
                                    if ((union146 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union146).getSchema().getFullName())) {
                                        (encoder).writeIndex(1);
                                        serializesubRecord135(((IndexedRecord) union146), (encoder));
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
        Map<CharSequence, List<IndexedRecord>> recordsMapArrayUnion147 = ((Map<CharSequence, List<IndexedRecord>> ) data.get(3));
        if (recordsMapArrayUnion147 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsMapArrayUnion147 instanceof Map) {
                (encoder).writeIndex(1);
                (encoder).writeMapStart();
                if ((((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion147) == null)||((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion147).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion147).size());
                    for (CharSequence key148 : ((Map<CharSequence, List<IndexedRecord>> )((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion147)).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key148);
                        List<IndexedRecord> array149 = null;
                        array149 = ((Map<CharSequence, List<IndexedRecord>> )((Map<CharSequence, List<IndexedRecord>> ) recordsMapArrayUnion147)).get(key148);
                        (encoder).writeArrayStart();
                        if ((array149 == null)||array149 .isEmpty()) {
                            (encoder).setItemCount(0);
                        } else {
                            (encoder).setItemCount(array149 .size());
                            for (int counter150 = 0; (counter150 <((List<IndexedRecord> ) array149).size()); counter150 ++) {
                                (encoder).startItem();
                                IndexedRecord union151 = null;
                                union151 = ((List<IndexedRecord> ) array149).get(counter150);
                                if (union151 == null) {
                                    (encoder).writeIndex(0);
                                    (encoder).writeNull();
                                } else {
                                    if ((union151 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union151).getSchema().getFullName())) {
                                        (encoder).writeIndex(1);
                                        serializesubRecord135(((IndexedRecord) union151), (encoder));
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
    public void serializesubRecord135(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField136 = ((CharSequence) data.get(0));
        if (subField136 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField136 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField136 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField136));
                } else {
                    (encoder).writeString(subField136 .toString());
                }
            }
        }
    }

}
