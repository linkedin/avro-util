
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

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
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        IndexedRecord record0 = ((IndexedRecord) data.get(0));
        if (record0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((record0 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) record0).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializeSubRecord0(((IndexedRecord) record0), (encoder));
            }
        }
        IndexedRecord record10 = ((IndexedRecord) data.get(1));
        serializeSubRecord0(record10, (encoder));
        CharSequence field0 = ((CharSequence) data.get(2));
        if (field0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (field0 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (field0 instanceof Utf8) {
                    (encoder).writeString(((Utf8) field0));
                } else {
                    (encoder).writeString(field0 .toString());
                }
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
            if (subField0 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField0 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField0));
                } else {
                    (encoder).writeString(subField0 .toString());
                }
            }
        }
    }

}
