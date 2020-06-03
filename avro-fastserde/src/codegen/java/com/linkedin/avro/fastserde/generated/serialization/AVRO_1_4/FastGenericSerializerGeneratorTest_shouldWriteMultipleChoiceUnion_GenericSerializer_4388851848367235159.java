
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion_GenericSerializer_4388851848367235159
    implements FastSerializer<IndexedRecord>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion99(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion99(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Object union100 = ((Object) data.get(0));
        if (union100 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((union100 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union100).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializesubRecord101(((IndexedRecord) union100), (encoder));
            } else {
                if (union100 instanceof CharSequence) {
                    (encoder).writeIndex(2);
                    if (union100 instanceof Utf8) {
                        (encoder).writeString(((Utf8) union100));
                    } else {
                        (encoder).writeString(union100 .toString());
                    }
                } else {
                    if (union100 instanceof Integer) {
                        (encoder).writeIndex(3);
                        (encoder).writeInt(((Integer) union100));
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializesubRecord101(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField102 = ((CharSequence) data.get(0));
        if (subField102 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField102 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField102 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField102));
                } else {
                    (encoder).writeString(subField102 .toString());
                }
            }
        }
    }

}
