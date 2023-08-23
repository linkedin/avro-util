
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteFixed_GenericSerializer_69494836
    implements FastSerializer<IndexedRecord>
{


    public FastGenericSerializerGeneratorTest_shouldWriteFixed_GenericSerializer_69494836() {
    }

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteFixed0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteFixed0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) data.get(0)).bytes());
        org.apache.avro.generic.GenericData.Fixed testFixedUnion0 = ((org.apache.avro.generic.GenericData.Fixed) data.get(1));
        if (testFixedUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) testFixedUnion0).bytes());
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray0 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(2));
        (encoder).writeArrayStart();
        Object array0 = testFixedArray0;
        if ((testFixedArray0 == null)||testFixedArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testFixedArray0 .size());
            for (int counter0 = 0; (counter0 <testFixedArray0 .size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) testFixedArray0 .get(counter0)).bytes());
            }
        }
        (encoder).writeArrayEnd();
        List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArray0 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(3));
        (encoder).writeArrayStart();
        Object array1 = testFixedUnionArray0;
        if ((testFixedUnionArray0 == null)||testFixedUnionArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testFixedUnionArray0 .size());
            for (int counter1 = 0; (counter1 <testFixedUnionArray0 .size()); counter1 ++) {
                (encoder).startItem();
                org.apache.avro.generic.GenericData.Fixed union0 = null;
                union0 = ((List<org.apache.avro.generic.GenericData.Fixed> ) testFixedUnionArray0).get(counter1);
                if (union0 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    (encoder).writeIndex(1);
                    (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) union0).bytes());
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
