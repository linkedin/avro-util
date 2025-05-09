
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps_GenericDeserializer_1605446505_1605446505
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema arrayField0;
    private final Schema arrayFieldArrayElemSchema0;
    private final Schema arrayFieldElemArrayElemSchema0;

    public FastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps_GenericDeserializer_1605446505_1605446505(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.arrayField0 = readerSchema.getField("arrayField").schema();
        this.arrayFieldArrayElemSchema0 = arrayField0 .getElementType();
        this.arrayFieldElemArrayElemSchema0 = arrayFieldArrayElemSchema0 .getElementType();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        List<List<Map<Utf8, Integer>>> arrayField1 = null;
        long chunkLen0 = (decoder.readArrayStart());
        Object oldArray0 = fastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0 .get(0);
        if (oldArray0 instanceof List) {
            arrayField1 = ((List) oldArray0);
            if (arrayField1 instanceof GenericArray) {
                ((GenericArray) arrayField1).reset();
            } else {
                arrayField1 .clear();
            }
        } else {
            arrayField1 = new org.apache.avro.generic.GenericData.Array<List<Map<Utf8, Integer>>>(((int) chunkLen0), arrayField0);
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object arrayFieldArrayElementReuseVar0 = null;
                if (oldArray0 instanceof GenericArray) {
                    arrayFieldArrayElementReuseVar0 = ((GenericArray) oldArray0).peek();
                }
                List<Map<Utf8, Integer>> arrayFieldElem0 = null;
                long chunkLen1 = (decoder.readArrayStart());
                if (arrayFieldArrayElementReuseVar0 instanceof List) {
                    arrayFieldElem0 = ((List) arrayFieldArrayElementReuseVar0);
                    if (arrayFieldElem0 instanceof GenericArray) {
                        ((GenericArray) arrayFieldElem0).reset();
                    } else {
                        arrayFieldElem0 .clear();
                    }
                } else {
                    arrayFieldElem0 = new org.apache.avro.generic.GenericData.Array<Map<Utf8, Integer>>(((int) chunkLen1), arrayFieldArrayElemSchema0);
                }
                while (chunkLen1 > 0) {
                    for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                        Object arrayFieldElemArrayElementReuseVar0 = null;
                        if (arrayFieldArrayElementReuseVar0 instanceof GenericArray) {
                            arrayFieldElemArrayElementReuseVar0 = ((GenericArray) arrayFieldArrayElementReuseVar0).peek();
                        }
                        Map<Utf8, Integer> arrayFieldElemElem0 = null;
                        long chunkLen2 = (decoder.readMapStart());
                        if (chunkLen2 > 0) {
                            arrayFieldElemElem0 = ((Map)(customization).getNewMapOverrideFunc().apply(arrayFieldElemArrayElementReuseVar0, ((int) chunkLen2)));
                            do {
                                for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                                    Utf8 key0 = (decoder.readString(null));
                                    arrayFieldElemElem0 .put(key0, (decoder.readInt()));
                                }
                                chunkLen2 = (decoder.mapNext());
                            } while (chunkLen2 > 0);
                        } else {
                            arrayFieldElemElem0 = ((Map)(customization).getNewMapOverrideFunc().apply(arrayFieldElemArrayElementReuseVar0, 0));
                        }
                        arrayFieldElem0 .add(arrayFieldElemElem0);
                    }
                    chunkLen1 = (decoder.arrayNext());
                }
                arrayField1 .add(arrayFieldElem0);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        fastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0 .put(0, arrayField1);
        return fastGenericDeserializerGeneratorTest_shouldReadNestedArrayOfMaps0;
    }

}
