
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.customized.DatumWriterCustomization;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastGenericSerializerGeneratorTest_shouldWriteSubRecordField_GenericSerializer_652769027
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField0(data, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteSubRecordField0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        IndexedRecord record0 = ((IndexedRecord) data.get(0));
        if (record0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((record0 instanceof IndexedRecord)&&"com.linkedin.avro.fastserde.generated.avro.subRecord".equals(((IndexedRecord) record0).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                serializeSubRecord0(((IndexedRecord) record0), (encoder), (customization));
            }
        }
        serialize_FastGenericSerializerGeneratorTest_shouldWriteSubRecordField0(data, (encoder), (customization));
    }

    @SuppressWarnings("unchecked")
    public void serializeSubRecord0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
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

    @SuppressWarnings("unchecked")
    private void serialize_FastGenericSerializerGeneratorTest_shouldWriteSubRecordField0(IndexedRecord data, Encoder encoder, DatumWriterCustomization customization)
        throws IOException
    {
        IndexedRecord record10 = ((IndexedRecord) data.get(1));
        serializeSubRecord0(record10, (encoder), (customization));
        CharSequence field0 = ((CharSequence) data.get(2));
        if (field0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            if (((CharSequence) field0) instanceof Utf8) {
                (encoder).writeString(((Utf8)((CharSequence) field0)));
            } else {
                (encoder).writeString(((CharSequence) field0).toString());
            }
        }
    }

}
