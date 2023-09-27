
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_9;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteRightUnionIndex_GenericSerializer_31578414
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteRightUnionIndex0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteRightUnionIndex0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Object union_field0 = ((Object) data.get(0));
        if (union_field0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((union_field0 instanceof IndexedRecord)&&"com.linkedin.avro.fastserde.generated.avro.record1".equals(((IndexedRecord) union_field0).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializeRecord10(((IndexedRecord) union_field0), (encoder));
            } else {
                if ((union_field0 instanceof IndexedRecord)&&"com.linkedin.avro.fastserde.generated.avro.record2".equals(((IndexedRecord) union_field0).getSchema().getFullName())) {
                    (encoder).writeIndex(2);
                    serializeRecord20(((IndexedRecord) union_field0), (encoder));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializeRecord10(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        if (((CharSequence) data.get(0)) instanceof Utf8) {
            (encoder).writeString(((Utf8)((CharSequence) data.get(0))));
        } else {
            (encoder).writeString(((CharSequence) data.get(0)).toString());
        }
    }

    @SuppressWarnings("unchecked")
    public void serializeRecord20(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        if (((CharSequence) data.get(0)) instanceof Utf8) {
            (encoder).writeString(((Utf8)((CharSequence) data.get(0))));
        } else {
            (encoder).writeString(((CharSequence) data.get(0)).toString());
        }
    }

}
