
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

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField_GenericSerializer_4869519538077502381
    implements FastSerializer<IndexedRecord>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField115(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordCollectionsField115(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<IndexedRecord> recordsArray116 = ((List<IndexedRecord> ) data.get(0));
        (encoder).writeArrayStart();
        if ((recordsArray116 == null)||recordsArray116 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsArray116 .size());
            for (int counter117 = 0; (counter117 <((List<IndexedRecord> ) recordsArray116).size()); counter117 ++) {
                (encoder).startItem();
                IndexedRecord subRecord118 = null;
                subRecord118 = ((List<IndexedRecord> ) recordsArray116).get(counter117);
                serializesubRecord119(subRecord118, (encoder));
            }
        }
        (encoder).writeArrayEnd();
        Map<CharSequence, IndexedRecord> recordsMap121 = ((Map<CharSequence, IndexedRecord> ) data.get(1));
        (encoder).writeMapStart();
        if ((recordsMap121 == null)||recordsMap121 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(recordsMap121 .size());
            for (CharSequence key122 : ((Map<CharSequence, IndexedRecord> ) recordsMap121).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key122);
                IndexedRecord subRecord123 = null;
                subRecord123 = ((Map<CharSequence, IndexedRecord> ) recordsMap121).get(key122);
                serializesubRecord119(subRecord123, (encoder));
            }
        }
        (encoder).writeMapEnd();
        List<IndexedRecord> recordsArrayUnion124 = ((List<IndexedRecord> ) data.get(2));
        if (recordsArrayUnion124 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsArrayUnion124 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<IndexedRecord> ) recordsArrayUnion124) == null)||((List<IndexedRecord> ) recordsArrayUnion124).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<IndexedRecord> ) recordsArrayUnion124).size());
                    for (int counter125 = 0; (counter125 <((List<IndexedRecord> )((List<IndexedRecord> ) recordsArrayUnion124)).size()); counter125 ++) {
                        (encoder).startItem();
                        IndexedRecord union126 = null;
                        union126 = ((List<IndexedRecord> )((List<IndexedRecord> ) recordsArrayUnion124)).get(counter125);
                        if (union126 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union126 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union126).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord119(((IndexedRecord) union126), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        Map<CharSequence, IndexedRecord> recordsMapUnion127 = ((Map<CharSequence, IndexedRecord> ) data.get(3));
        if (recordsMapUnion127 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (recordsMapUnion127 instanceof Map) {
                (encoder).writeIndex(1);
                (encoder).writeMapStart();
                if ((((Map<CharSequence, IndexedRecord> ) recordsMapUnion127) == null)||((Map<CharSequence, IndexedRecord> ) recordsMapUnion127).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((Map<CharSequence, IndexedRecord> ) recordsMapUnion127).size());
                    for (CharSequence key128 : ((Map<CharSequence, IndexedRecord> )((Map<CharSequence, IndexedRecord> ) recordsMapUnion127)).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key128);
                        IndexedRecord union129 = null;
                        union129 = ((Map<CharSequence, IndexedRecord> )((Map<CharSequence, IndexedRecord> ) recordsMapUnion127)).get(key128);
                        if (union129 == null) {
                            (encoder).writeIndex(0);
                            (encoder).writeNull();
                        } else {
                            if ((union129 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union129).getSchema().getFullName())) {
                                (encoder).writeIndex(1);
                                serializesubRecord119(((IndexedRecord) union129), (encoder));
                            }
                        }
                    }
                }
                (encoder).writeMapEnd();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializesubRecord119(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField120 = ((CharSequence) data.get(0));
        if (subField120 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField120 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField120 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField120));
                } else {
                    (encoder).writeString(subField120 .toString());
                }
            }
        }
    }

}
