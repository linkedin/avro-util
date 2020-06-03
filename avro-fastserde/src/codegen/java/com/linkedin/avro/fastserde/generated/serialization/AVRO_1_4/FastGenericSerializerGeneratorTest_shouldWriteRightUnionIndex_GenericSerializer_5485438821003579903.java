
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteRightUnionIndex_GenericSerializer_5485438821003579903
    implements FastSerializer<IndexedRecord>
{

    private Map<Long, Schema> enumSchemaMap = new ConcurrentHashMap<Long, Schema>();

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteRightUnionIndex111(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteRightUnionIndex111(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        Object union_field112 = ((Object) data.get(0));
        if (union_field112 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((union_field112 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.record1".equals(((IndexedRecord) union_field112).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializerecord1113(((IndexedRecord) union_field112), (encoder));
            } else {
                if ((union_field112 instanceof IndexedRecord)&&"com.adpilot.utils.generated.avro.record2".equals(((IndexedRecord) union_field112).getSchema().getFullName())) {
                    (encoder).writeIndex(2);
                    serializerecord2114(((IndexedRecord) union_field112), (encoder));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void serializerecord1113(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        if (data.get(0) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(0)));
        } else {
            (encoder).writeString(data.get(0).toString());
        }
    }

    @SuppressWarnings("unchecked")
    public void serializerecord2114(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        if (data.get(0) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(0)));
        } else {
            (encoder).writeString(data.get(0).toString());
        }
    }

}
