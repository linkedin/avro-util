
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteFixed_GenericSerializer_8889056593487745201
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteFixed76(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteFixed76(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) data.get(0)).bytes());
        org.apache.avro.generic.GenericData.Fixed testFixedUnion77 = ((org.apache.avro.generic.GenericData.Fixed) data.get(1));
        if (testFixedUnion77 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testFixedUnion77 instanceof org.apache.avro.generic.GenericData.Fixed) {
                (encoder).writeIndex(1);
                (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) testFixedUnion77).bytes());
            }
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray78 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(2));
        (encoder).writeArrayStart();
        if ((testFixedArray78 == null)||testFixedArray78 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testFixedArray78 .size());
            for (int counter79 = 0; (counter79 <((List<org.apache.avro.generic.GenericData.Fixed> ) testFixedArray78).size()); counter79 ++) {
                (encoder).startItem();
                (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) testFixedArray78 .get(counter79)).bytes());
            }
        }
        (encoder).writeArrayEnd();
        List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArray80 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(3));
        (encoder).writeArrayStart();
        if ((testFixedUnionArray80 == null)||testFixedUnionArray80 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testFixedUnionArray80 .size());
            for (int counter81 = 0; (counter81 <((List<org.apache.avro.generic.GenericData.Fixed> ) testFixedUnionArray80).size()); counter81 ++) {
                (encoder).startItem();
                org.apache.avro.generic.GenericData.Fixed union82 = null;
                union82 = ((List<org.apache.avro.generic.GenericData.Fixed> ) testFixedUnionArray80).get(counter81);
                if (union82 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if (union82 instanceof org.apache.avro.generic.GenericData.Fixed) {
                        (encoder).writeIndex(1);
                        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) union82).bytes());
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
