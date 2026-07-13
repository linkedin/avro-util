
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class OuterRecordWithNestedNullableComplexFields_GenericDeserializer_1244262185_49792023
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema innerRecord0;
    private final Schema innerRecordNullableRecordSchema0;
    private final Schema innerMap0;
    private final Schema innerMapMapSchema0;
    private final Schema innerArray0;
    private final Schema innerArrayArraySchema0;

    public OuterRecordWithNestedNullableComplexFields_GenericDeserializer_1244262185_49792023(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.innerRecord0 = readerSchema.getField("innerRecord").schema();
        this.innerRecordNullableRecordSchema0 = innerRecord0 .getTypes().get(1);
        this.innerMap0 = readerSchema.getField("innerMap").schema();
        this.innerMapMapSchema0 = innerMap0 .getTypes().get(1);
        this.innerArray0 = readerSchema.getField("innerArray").schema();
        this.innerArrayArraySchema0 = innerArray0 .getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeOuterRecordWithNestedNullableComplexFields0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeOuterRecordWithNestedNullableComplexFields0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord outerRecordWithNestedNullableComplexFields0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            outerRecordWithNestedNullableComplexFields0 = ((IndexedRecord)(reuse));
        } else {
            outerRecordWithNestedNullableComplexFields0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        outerRecordWithNestedNullableComplexFields0 .put(0, deserializeInnerRecordNullable0(outerRecordWithNestedNullableComplexFields0 .get(0), (decoder), (customization)));
        populate_OuterRecordWithNestedNullableComplexFields0((outerRecordWithNestedNullableComplexFields0), (customization), (decoder));
        return outerRecordWithNestedNullableComplexFields0;
    }

    public IndexedRecord deserializeInnerRecordNullable0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord innerRecordNullable0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == innerRecordNullableRecordSchema0)) {
            innerRecordNullable0 = ((IndexedRecord)(reuse));
        } else {
            innerRecordNullable0 = new org.apache.avro.generic.GenericData.Record(innerRecordNullableRecordSchema0);
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

    private void populate_OuterRecordWithNestedNullableComplexFields0(IndexedRecord outerRecordWithNestedNullableComplexFields0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        Map<Utf8, Integer> innerMap1 = null;
        long chunkLen0 = (decoder.readMapStart());
        if (chunkLen0 > 0) {
            innerMap1 = ((Map)(customization).getNewMapOverrideFunc().apply(outerRecordWithNestedNullableComplexFields0 .get(1), ((int) chunkLen0)));
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    innerMap1 .put(key0, (decoder.readInt()));
                }
                chunkLen0 = (decoder.mapNext());
            } while (chunkLen0 > 0);
        } else {
            innerMap1 = ((Map)(customization).getNewMapOverrideFunc().apply(outerRecordWithNestedNullableComplexFields0 .get(1), 0));
        }
        outerRecordWithNestedNullableComplexFields0 .put(1, innerMap1);
        PrimitiveIntList innerArray1 = null;
        long chunkLen1 = (decoder.readArrayStart());
        Object oldArray0 = outerRecordWithNestedNullableComplexFields0 .get(2);
        if (oldArray0 instanceof PrimitiveIntList) {
            innerArray1 = ((PrimitiveIntList) oldArray0);
            innerArray1 .clear();
        } else {
            innerArray1 = new PrimitiveIntArrayList(((int) chunkLen1));
        }
        while (chunkLen1 > 0) {
            for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                innerArray1 .addPrimitive((decoder.readInt()));
            }
            chunkLen1 = (decoder.arrayNext());
        }
        outerRecordWithNestedNullableComplexFields0 .put(2, innerArray1);
    }

}
