
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

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
        List<Double> array743 = null;
        long chunkLen744 = (decoder.readArrayStart());
        if (chunkLen744 > 0) {
            List<Double> arrayReuse745 = null;
            if ((reuse) instanceof List) {
                arrayReuse745 = ((List)(reuse));
            }
            if (arrayReuse745 != (null)) {
                arrayReuse745 .clear();
                array743 = arrayReuse745;
            } else {
                array743 = new org.apache.avro.generic.GenericData.Array<Double>(((int) chunkLen744), readerSchema);
            }
            do {
                for (int counter746 = 0; (counter746 <chunkLen744); counter746 ++) {
                    Object arrayArrayElementReuseVar747 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar747 = ((GenericArray)(reuse)).peek();
                    }
                    array743 .add((decoder.readDouble()));
                }
                chunkLen744 = (decoder.arrayNext());
            } while (chunkLen744 > 0);
        } else {
            array743 = new org.apache.avro.generic.GenericData.Array<Double>(0, readerSchema);
        }
        return array743;
    }

}
