
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class TestArrayOfFloats_GenericSerializer_1408566797
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeTestArrayOfFloats0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeTestArrayOfFloats0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<Float> array_of_float0 = ((List<Float> ) data.get(0));
        (encoder).writeArrayStart();
        if ((array_of_float0 == null)||array_of_float0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(array_of_float0 .size());
            Object array0 = array_of_float0;
            if (array0 instanceof PrimitiveFloatList) {
                PrimitiveFloatList primitiveList0 = ((PrimitiveFloatList) array0);
                if (primitiveList0 instanceof BufferBackedPrimitiveFloatList) {
                    BufferBackedPrimitiveFloatList bufferBackedPrimitiveFloatList = ((BufferBackedPrimitiveFloatList) primitiveList0);
                    bufferBackedPrimitiveFloatList.writeFloats((encoder));
                } else {
                    for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                        (encoder).startItem();
                        (encoder).writeFloat(primitiveList0 .getPrimitive(counter0));
                    }
                }
            } else {
                for (int counter1 = 0; (counter1 <array_of_float0 .size()); counter1 ++) {
                    (encoder).startItem();
                    (encoder).writeFloat(array_of_float0 .get(counter1));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
