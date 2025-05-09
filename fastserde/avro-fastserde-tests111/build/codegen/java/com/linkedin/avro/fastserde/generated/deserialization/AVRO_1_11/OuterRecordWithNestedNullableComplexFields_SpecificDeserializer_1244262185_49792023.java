
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.InnerRecordNullable;
import com.linkedin.avro.fastserde.generated.avro.OuterRecordWithNestedNullableComplexFields;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class OuterRecordWithNestedNullableComplexFields_SpecificDeserializer_1244262185_49792023
    implements FastDeserializer<OuterRecordWithNestedNullableComplexFields>
{

    private final Schema readerSchema;

    public OuterRecordWithNestedNullableComplexFields_SpecificDeserializer_1244262185_49792023(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public OuterRecordWithNestedNullableComplexFields deserialize(OuterRecordWithNestedNullableComplexFields reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeOuterRecordWithNestedNullableComplexFields0((reuse), (decoder), (customization));
    }

    public OuterRecordWithNestedNullableComplexFields deserializeOuterRecordWithNestedNullableComplexFields0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        OuterRecordWithNestedNullableComplexFields outerRecordWithNestedNullableComplexFields0;
        if ((reuse)!= null) {
            outerRecordWithNestedNullableComplexFields0 = ((OuterRecordWithNestedNullableComplexFields)(reuse));
        } else {
            outerRecordWithNestedNullableComplexFields0 = new OuterRecordWithNestedNullableComplexFields();
        }
        outerRecordWithNestedNullableComplexFields0 .put(0, deserializeInnerRecordNullable0(outerRecordWithNestedNullableComplexFields0 .get(0), (decoder), (customization)));
        populate_OuterRecordWithNestedNullableComplexFields0((outerRecordWithNestedNullableComplexFields0), (customization), (decoder));
        return outerRecordWithNestedNullableComplexFields0;
    }

    public InnerRecordNullable deserializeInnerRecordNullable0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        InnerRecordNullable innerRecordNullable0;
        if ((reuse)!= null) {
            innerRecordNullable0 = ((InnerRecordNullable)(reuse));
        } else {
            innerRecordNullable0 = new InnerRecordNullable();
        }
        Utf8 charSequence0;
        Object oldString0 = innerRecordNullable0 .get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        innerRecordNullable0 .put(0, charSequence0);
        return innerRecordNullable0;
    }

    private void populate_OuterRecordWithNestedNullableComplexFields0(OuterRecordWithNestedNullableComplexFields outerRecordWithNestedNullableComplexFields0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        Map<Utf8, Integer> innerMap0 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            innerMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(outerRecordWithNestedNullableComplexFields0 .get(1), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    innerMap0 .put(key0, (decoder.readInt()));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            innerMap0 = ((Map)(customization).getNewMapOverrideFunc().apply(outerRecordWithNestedNullableComplexFields0 .get(1), 0));
        }
        outerRecordWithNestedNullableComplexFields0 .put(1, innerMap0);
        PrimitiveIntList innerArray0 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray0 = outerRecordWithNestedNullableComplexFields0 .get(2);
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
        outerRecordWithNestedNullableComplexFields0 .put(2, innerArray0);
    }

}
