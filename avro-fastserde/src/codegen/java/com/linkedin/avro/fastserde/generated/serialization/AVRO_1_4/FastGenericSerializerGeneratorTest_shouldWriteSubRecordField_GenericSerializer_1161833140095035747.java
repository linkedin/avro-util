
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordField_GenericSerializer_1161833140095035747
    implements FastSerializer<IndexedRecord>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField153(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField153(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        IndexedRecord record154 = ((IndexedRecord) data.get(0));
        if (record154 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((record154 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) record154).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializesubRecord155(((IndexedRecord) record154), (encoder));
            }
        }
        IndexedRecord record1157 = ((IndexedRecord) data.get(1));
        serializesubRecord155(record1157, (encoder));
        CharSequence field158 = ((CharSequence) data.get(2));
        if (field158 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field158 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field158 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field158));
                } else {
                    (encoder).writeString(field158 .toString());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializesubRecord155(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField156 = ((CharSequence) data.get(0));
        if (subField156 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField156 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField156 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField156));
                } else {
                    (encoder).writeString(subField156 .toString());
                }
            }
        }
    }

}
