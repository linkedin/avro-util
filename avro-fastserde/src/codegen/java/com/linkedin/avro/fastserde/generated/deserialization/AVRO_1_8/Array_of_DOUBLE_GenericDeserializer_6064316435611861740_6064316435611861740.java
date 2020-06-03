
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class Array_of_DOUBLE_GenericDeserializer_6064316435611861740_6064316435611861740
    implements FastDeserializer<List<Double>>
{

    private final Schema readerSchema;

    public Array_of_DOUBLE_GenericDeserializer_6064316435611861740_6064316435611861740(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Double> deserialize(List<Double> reuse, Decoder decoder)
        throws IOException
    {
        List<Double> array597 = null;
        long chunkLen598 = (decoder.readArrayStart());
        if (chunkLen598 > 0) {
            List<Double> arrayReuse599 = null;
            if ((reuse) instanceof List) {
                arrayReuse599 = ((List)(reuse));
            }
            if (arrayReuse599 != (null)) {
                arrayReuse599 .clear();
                array597 = arrayReuse599;
            } else {
                array597 = new org.apache.avro.generic.GenericData.Array<Double>(((int) chunkLen598), readerSchema);
            }
            do {
                for (int counter600 = 0; (counter600 <chunkLen598); counter600 ++) {
                    Object arrayArrayElementReuseVar601 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar601 = ((GenericArray)(reuse)).peek();
                    }
                    array597 .add((decoder.readDouble()));
                }
                chunkLen598 = (decoder.arrayNext());
            } while (chunkLen598 > 0);
        } else {
            array597 = new org.apache.avro.generic.GenericData.Array<Double>(0, readerSchema);
        }
        return array597;
    }

}
