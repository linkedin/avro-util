
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField_GenericSerializer_7124257652769599022
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField114(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField114(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<IndexedRecord> recordsArray115 = ((List<IndexedRecord> ) data.get(0));
        (encoder).writeArrayStart();
        if ((recordsArray115 == null)||recordsArray115 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsArray115 .size());
            for (int counter116 = 0; (counter116 <((List<IndexedRecord> ) recordsArray115).size()); counter116 ++) {
                (encoder).startItem();
                IndexedRecord subRecord117 = null;
                subRecord117 = ((List<IndexedRecord> ) recordsArray115).get(counter116);
                serializesubRecord118(subRecord117, (encoder));
            }
        }
        (encoder).writeArrayEnd();
        Map<CharSequence, IndexedRecord> recordsMap120 = ((Map<CharSequence, IndexedRecord> ) data.get(1));
        (encoder).writeMapStart();
        if ((recordsMap120 == null)||recordsMap120 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsMap120 .size());
            for (CharSequence key121 : ((Map<CharSequence, IndexedRecord> ) recordsMap120).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key121);
                IndexedRecord subRecord122 = null;
                subRecord122 = ((Map<CharSequence, IndexedRecord> ) recordsMap120).get(key121);
                serializesubRecord118(subRecord122, (encoder));
            }
        }
        (encoder).writeMapEnd();
        List<IndexedRecord> recordsArrayUnion123 = ((List<IndexedRecord> ) data.get(2));
        if (recordsArrayUnion123 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsArrayUnion123 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<IndexedRecord> ) recordsArrayUnion123) == null)||((List<IndexedRecord> ) recordsArrayUnion123).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<IndexedRecord> ) recordsArrayUnion123).size());
                    for (int counter124 = 0; (counter124 <((List<IndexedRecord> )((List<IndexedRecord> ) recordsArrayUnion123)).size()); counter124 ++) {
                        (encoder).startItem();
                        IndexedRecord union125 = null;
                        union125 = ((List<IndexedRecord> )((List<IndexedRecord> ) recordsArrayUnion123)).get(counter124);
                        if (union125 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union125 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union125).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord118(((IndexedRecord) union125), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        Map<CharSequence, IndexedRecord> recordsMapUnion126 = ((Map<CharSequence, IndexedRecord> ) data.get(3));
        if (recordsMapUnion126 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsMapUnion126 instanceof Map) {
                (encoder).writeIndex(1);
                (encoder).writeMapStart();
                if ((((Map<CharSequence, IndexedRecord> ) recordsMapUnion126) == null)||((Map<CharSequence, IndexedRecord> ) recordsMapUnion126).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((Map<CharSequence, IndexedRecord> ) recordsMapUnion126).size());
                    for (CharSequence key127 : ((Map<CharSequence, IndexedRecord> )((Map<CharSequence, IndexedRecord> ) recordsMapUnion126)).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key127);
                        IndexedRecord union128 = null;
                        union128 = ((Map<CharSequence, IndexedRecord> )((Map<CharSequence, IndexedRecord> ) recordsMapUnion126)).get(key127);
                        if (union128 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union128 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union128).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord118(((IndexedRecord) union128), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeMapEnd();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializesubRecord118(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField119 = ((CharSequence) data.get(0));
        if (subField119 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField119 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField119 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField119));
                } else {
                    (encoder).writeString(subField119 .toString());
                }
            }
        }
    }

}
