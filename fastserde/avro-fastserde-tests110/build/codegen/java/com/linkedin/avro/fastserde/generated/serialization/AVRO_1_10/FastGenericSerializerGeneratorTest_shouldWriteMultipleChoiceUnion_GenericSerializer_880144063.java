
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_10;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion_GenericSerializer_880144063
    implements FastSerializer<IndexedRecord>
{

    private final GenericData modelData;

    public FastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion_GenericSerializer_880144063(GenericData modelData) {
        this.modelData = modelData;
    }

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteMultipleChoiceUnion0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Object union0 = ((Object) data.get(0));
        if (union0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((union0 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.subRecord".equals(((IndexedRecord) union0).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializeSubRecord0(((IndexedRecord) union0), (encoder));
            } else {
                if (union0 instanceof CharSequence) {
                    (encoder).writeIndex(2);
                    if (((CharSequence) union0) instanceof Utf8) {
                        (encoder).writeString(((Utf8)((CharSequence) union0)));
                    } else {
                        (encoder).writeString(((CharSequence) union0).toString());
                    }
                } else {
                    if (union0 instanceof Integer) {
                        (encoder).writeIndex(3);
                        (encoder).writeInt(((Integer) union0));
                    }
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
            (encoder).writeIndex(1);
            if (((CharSequence) subField0) instanceof Utf8) {
                (encoder).writeString(((Utf8)((CharSequence) subField0)));
            } else {
                (encoder).writeString(((CharSequence) subField0).toString());
            }
        }
    }

}
