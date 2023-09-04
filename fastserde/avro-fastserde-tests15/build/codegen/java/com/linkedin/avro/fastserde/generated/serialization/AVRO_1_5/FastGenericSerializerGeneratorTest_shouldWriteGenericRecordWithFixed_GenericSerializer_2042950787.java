
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_5;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithFixed_GenericSerializer_2042950787
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithFixed0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithFixed0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) data.get(0)).bytes());
        serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithFixed0(data, (encoder));
        serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithFixed1(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithFixed0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        org.apache.avro.generic.GenericData.Fixed testFixedUnion0 = ((org.apache.avro.generic.GenericData.Fixed) data.get(1));
        if (testFixedUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((testFixedUnion0 instanceof org.apache.avro.generic.GenericData.Fixed)&&"com.adpilot.utils.generated.avro.testFixed".equals(((org.apache.avro.generic.GenericData.Fixed) testFixedUnion0).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) testFixedUnion0).bytes());
            }
        }
        List<org.apache.avro.generic.GenericData.Fixed> testFixedArray0 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(2));
        (encoder).writeArrayStart();
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
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithFixed1(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<org.apache.avro.generic.GenericData.Fixed> testFixedUnionArray0 = ((List<org.apache.avro.generic.GenericData.Fixed> ) data.get(3));
        (encoder).writeArrayStart();
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
                    if ((union0 instanceof org.apache.avro.generic.GenericData.Fixed)&&"com.adpilot.utils.generated.avro.testFixed".equals(((org.apache.avro.generic.GenericData.Fixed) union0).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        (encoder).writeFixed(((org.apache.avro.generic.GenericData.Fixed) union0).bytes());
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
