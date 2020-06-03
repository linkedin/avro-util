
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion_GenericSerializer_94676278194882652
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion98(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion98(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Object union99 = ((Object) data.get(0));
        if (union99 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((union99 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union99).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializesubRecord100(((IndexedRecord) union99), (encoder));
            } else {
                if (union99 instanceof CharSequence) {
                    (encoder).writeIndex(2);
                    if (union99 instanceof Utf8) {
                        (encoder).writeString(((Utf8) union99));
                    } else {
                        (encoder).writeString(union99 .toString());
                    }
                } else {
                    if (union99 instanceof Integer) {
                        (encoder).writeIndex(3);
                        (encoder).writeInt(((Integer) union99));
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializesubRecord100(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        CharSequence subField101 = ((CharSequence) data.get(0));
        if (subField101 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (subField101 instanceof CharSequence) {
                (encoder).writeIndex(1);
                if (subField101 instanceof Utf8) {
                    (encoder).writeString(((Utf8) subField101));
                } else {
                    (encoder).writeString(subField101 .toString());
                }
            }
        }
    }

}
