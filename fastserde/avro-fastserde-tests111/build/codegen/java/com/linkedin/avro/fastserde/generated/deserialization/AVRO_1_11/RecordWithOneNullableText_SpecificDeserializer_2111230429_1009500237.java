
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.RecordWithOneNullableText;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class RecordWithOneNullableText_SpecificDeserializer_2111230429_1009500237
    implements FastDeserializer<RecordWithOneNullableText>
{

    private final Schema readerSchema;

    public RecordWithOneNullableText_SpecificDeserializer_2111230429_1009500237(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public RecordWithOneNullableText deserialize(RecordWithOneNullableText reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeRecordWithOneNullableText0((reuse), (decoder), (customization));
    }

    public RecordWithOneNullableText deserializeRecordWithOneNullableText0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        RecordWithOneNullableText RecordWithOneNullableTextAndDeeplyNestedRecord;
        if ((reuse)!= null) {
            RecordWithOneNullableTextAndDeeplyNestedRecord = ((RecordWithOneNullableText)(reuse));
        } else {
            RecordWithOneNullableTextAndDeeplyNestedRecord = new RecordWithOneNullableText();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            RecordWithOneNullableTextAndDeeplyNestedRecord.put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = RecordWithOneNullableTextAndDeeplyNestedRecord.get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                RecordWithOneNullableTextAndDeeplyNestedRecord.put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'text': "+ unionIndex0));
            }
        }
        populate_RecordWithOneNullableTextAndDeeplyNestedRecord0((RecordWithOneNullableTextAndDeeplyNestedRecord), (customization), (decoder));
        return RecordWithOneNullableTextAndDeeplyNestedRecord;
    }

    private void populate_RecordWithOneNullableTextAndDeeplyNestedRecord0(RecordWithOneNullableText RecordWithOneNullableTextAndDeeplyNestedRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex1 == 1) {
                deserializeNestedRecord0(null, (decoder), (customization));
            } else {
                throw new RuntimeException(("Illegal union index for 'nestedField': "+ unionIndex1));
            }
        }
    }

    public void deserializeNestedRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex2 == 1) {
                decoder.skipString();
            } else {
                throw new RuntimeException(("Illegal union index for 'sampleText1': "+ unionIndex2));
            }
        }
        populate_NestedRecord0((customization), (decoder));
    }

    private void populate_NestedRecord0(DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex3 == 1) {
                deserializeDeeplyNestedRecord0(null, (decoder), (customization));
            } else {
                throw new RuntimeException(("Illegal union index for 'deeplyNestedField': "+ unionIndex3));
            }
        }
    }

    public void deserializeDeeplyNestedRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex4 == 1) {
                decoder.skipString();
            } else {
                throw new RuntimeException(("Illegal union index for 'deeplyDeeplyNestedText': "+ unionIndex4));
            }
        }
    }

}
