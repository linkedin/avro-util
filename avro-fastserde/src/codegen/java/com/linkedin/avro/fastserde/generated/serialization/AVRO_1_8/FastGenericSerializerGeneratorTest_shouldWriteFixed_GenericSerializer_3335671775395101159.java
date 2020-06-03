
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteFixed_GenericSerializer_3335671775395101159
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteFixed75(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteFixed75(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) data.get(0)).bytes());
        org.apache.avro.generic.GenericData.Fixed testFixedUnion76 = ((org.apache.avro.generic.GenericData.Fixed) data.get(1));
        if (testFixedUnion76 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((testFixedUnion76 instanceof org.apache.avro.generic.GenericData.Fixed)&&"com.adpilot.utils.generated.avro.testFixed".equals(((org.apache.avro.generic.GenericData.Fixed) testFixedUnion76).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) testFixedUnion76).bytes());
            }
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray77 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(2));
        (encoder).writeArrayStart();
        if ((testFixedArray77 == null)||testFixedArray77 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testFixedArray77 .size());
            for (int counter78 = 0; (counter78 <((List<org.apache.avro.generic.GenericData.Fixed> ) testFixedArray77).size()); counter78 ++) {
                (encoder).startItem();
                (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) testFixedArray77 .get(counter78)).bytes());
            }
        }
        (encoder).writeArrayEnd();
        List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArray79 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(3));
        (encoder).writeArrayStart();
        if ((testFixedUnionArray79 == null)||testFixedUnionArray79 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testFixedUnionArray79 .size());
            for (int counter80 = 0; (counter80 <((List<org.apache.avro.generic.GenericData.Fixed> ) testFixedUnionArray79).size()); counter80 ++) {
                (encoder).startItem();
                org.apache.avro.generic.GenericData.Fixed union81 = null;
                union81 = ((List<org.apache.avro.generic.GenericData.Fixed> ) testFixedUnionArray79).get(counter80);
                if (union81 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union81 instanceof org.apache.avro.generic.GenericData.Fixed)&&"com.adpilot.utils.generated.avro.testFixed".equals(((org.apache.avro.generic.GenericData.Fixed) union81).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) union81).bytes());
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
