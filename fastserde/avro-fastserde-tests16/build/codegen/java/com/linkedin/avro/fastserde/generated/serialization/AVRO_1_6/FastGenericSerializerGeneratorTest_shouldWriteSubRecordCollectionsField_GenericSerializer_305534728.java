
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_6;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField_GenericSerializer_305534728
    implements FastSerializer<IndexedRecord>
{


    public FastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField_GenericSerializer_305534728() {
    }

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<IndexedRecord> recordsArray0 = ((List<IndexedRecord> ) data.get(0));
        (encoder).writeArrayStart();
        Object array0 = recordsArray0;
        if ((recordsArray0 == null)||recordsArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsArray0 .size());
            for (int counter0 = 0; (counter0 <recordsArray0 .size()); counter0 ++) {
                (encoder).startItem();
                IndexedRecord subRecord0 = null;
                subRecord0 = ((List<IndexedRecord> ) recordsArray0).get(counter0);
                serializeSubRecord0(subRecord0, (encoder));
            }
        }
        (encoder).writeArrayEnd();
        Map<CharSequence, IndexedRecord> recordsMap0 = ((Map<CharSequence, IndexedRecord> ) data.get(1));
        (encoder).writeMapStart();
        if ((recordsMap0 == null)||recordsMap0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsMap0 .size());
            for (CharSequence key0 : ((Map<CharSequence, IndexedRecord> ) recordsMap0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                IndexedRecord subRecord1 = null;
                subRecord1 = ((Map<CharSequence, IndexedRecord> ) recordsMap0).get(key0);
                serializeSubRecord0(subRecord1, (encoder));
            }
        }
        (encoder).writeMapEnd();
        List<IndexedRecord> recordsArrayUnion0 = ((List<IndexedRecord> ) data.get(2));
        if (recordsArrayUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsArrayUnion0 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                Object array1 = ((List<IndexedRecord> ) recordsArrayUnion0);
                if ((((List<IndexedRecord> ) recordsArrayUnion0) == null)||((List<IndexedRecord> ) recordsArrayUnion0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<IndexedRecord> ) recordsArrayUnion0).size());
                    for (int counter1 = 0; (counter1 <((List<IndexedRecord> ) recordsArrayUnion0).size()); counter1 ++) {
                        (encoder).startItem();
                        IndexedRecord union0 = null;
                        union0 = ((List<IndexedRecord> )((List<IndexedRecord> ) recordsArrayUnion0)).get(counter1);
                        if (union0 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union0 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union0).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializeSubRecord0(((IndexedRecord) union0), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        Map<CharSequence, IndexedRecord> recordsMapUnion0 = ((Map<CharSequence, IndexedRecord> ) data.get(3));
        if (recordsMapUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsMapUnion0 instanceof Map) {
                (encoder).writeIndex(1);
                (encoder).writeMapStart();
                if ((((Map<CharSequence, IndexedRecord> ) recordsMapUnion0) == null)||((Map<CharSequence, IndexedRecord> ) recordsMapUnion0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((Map<CharSequence, IndexedRecord> ) recordsMapUnion0).size());
                    for (CharSequence key1 : ((Map<CharSequence, IndexedRecord> )((Map<CharSequence, IndexedRecord> ) recordsMapUnion0)).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key1);
                        IndexedRecord union1 = null;
                        union1 = ((Map<CharSequence, IndexedRecord> )((Map<CharSequence, IndexedRecord> ) recordsMapUnion0)).get(key1);
                        if (union1 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union1 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union1).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializeSubRecord0(((IndexedRecord) union1), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeMapEnd();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializeSubRecord0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField0 = ((CharSequence) data.get(0));
        if (subField0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (((CharSequence) subField0) instanceof Utf8) {
                (encoder).writeString(((Utf8)((CharSequence) subField0)));
            } else {
                (encoder).writeString(((CharSequence) subField0).toString());
            }
        }
    }

}
