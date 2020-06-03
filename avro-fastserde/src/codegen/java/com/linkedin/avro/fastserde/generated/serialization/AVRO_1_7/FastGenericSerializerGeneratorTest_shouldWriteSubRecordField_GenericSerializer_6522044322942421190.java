
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordField_GenericSerializer_6522044322942421190
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField152(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField152(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        IndexedRecord record153 = ((IndexedRecord) data.get(0));
        if (record153 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((record153 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) record153).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializesubRecord154(((IndexedRecord) record153), (encoder));
            }
        }
        IndexedRecord record1156 = ((IndexedRecord) data.get(1));
        serializesubRecord154(record1156, (encoder));
        CharSequence field157 = ((CharSequence) data.get(2));
        if (field157 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field157 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field157 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field157));
                } else {
                    (encoder).writeString(field157 .toString());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializesubRecord154(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField155 = ((CharSequence) data.get(0));
        if (subField155 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField155 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField155 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField155));
                } else {
                    (encoder).writeString(subField155 .toString());
                }
            }
        }
    }

}
