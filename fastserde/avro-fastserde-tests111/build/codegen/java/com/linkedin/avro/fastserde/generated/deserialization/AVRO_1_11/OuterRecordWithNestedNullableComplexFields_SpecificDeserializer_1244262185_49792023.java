
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class OuterRecordWithNestedNullableComplexFields_SpecificDeserializer_1244262185_49792023
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields>
{

    private final Schema readerSchema;

    public OuterRecordWithNestedNullableComplexFields_SpecificDeserializer_1244262185_49792023(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields deserialize(com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeOuterRecordWithNestedNullableComplexFields0((reuse), (decoder), (customization));
    }

    public com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields deserializeOuterRecordWithNestedNullableComplexFields0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields OuterRecordWithNestedNullableComplexFields;
        if ((reuse)!= null) {
            OuterRecordWithNestedNullableComplexFields = ((com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields)(reuse));
        } else {
            OuterRecordWithNestedNullableComplexFields = new com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields();
        }
        OuterRecordWithNestedNullableComplexFields.put(0, deserializeInnerRecordNullable0(OuterRecordWithNestedNullableComplexFields.get(0), (decoder), (customization)));
        populate_OuterRecordWithNestedNullableComplexFields0((OuterRecordWithNestedNullableComplexFields), (customization), (decoder));
        return OuterRecordWithNestedNullableComplexFields;
    }

    public com.linkedin.avro.fastserde.generated.avro.InnerRecordNullable deserializeInnerRecordNullable0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.InnerRecordNullable InnerRecordNullable;
        if ((reuse)!= null) {
            InnerRecordNullable = ((com.linkedin.avro.fastserde.generated.avro.InnerRecordNullable)(reuse));
        } else {
            InnerRecordNullable = new com.linkedin.avro.fastserde.generated.avro.InnerRecordNullable();
        }
        Utf8 charSequence0;
        Object oldString0 = InnerRecordNullable.get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        InnerRecordNullable.put(0, charSequence0);
        return InnerRecordNullable;
    }

    private void populate_OuterRecordWithNestedNullableComplexFields0(com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields OuterRecordWithNestedNullableComplexFields, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        Map<Utf8, Integer> innerMap0 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            innerMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(OuterRecordWithNestedNullableComplexFields.get(1), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    innerMap0 .put(key0, (decoder.readInt()));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            innerMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(OuterRecordWithNestedNullableComplexFields.get(1), 0));
        }
        OuterRecordWithNestedNullableComplexFields.put(1, innerMap0);
        PrimitiveIntList innerArray0 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray0 = OuterRecordWithNestedNullableComplexFields.get(2);
        if (oldArray0 instanceof PrimitiveIntList) {
            innerArray0 = ((PrimitiveIntList) oldArray0);
            innerArray0 .clear();
        } else {
            innerArray0 = new PrimitiveIntArrayList(((int) chunkLen1));
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                innerArray0 .addPrimitive((decoder.readInt()));
            }
            chunkLen1 = (decoder.arrayNext());
        }
        OuterRecordWithNestedNullableComplexFields.put(2, innerArray0);
    }

}
