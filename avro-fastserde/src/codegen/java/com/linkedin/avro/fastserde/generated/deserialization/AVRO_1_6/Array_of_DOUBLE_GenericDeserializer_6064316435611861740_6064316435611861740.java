
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveDoubleList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.primitive.PrimitiveDoubleArrayList;
import org.apache.avro.Schema;
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
        PrimitiveDoubleList array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if ((reuse) instanceof PrimitiveDoubleList) {
            array0 = ((PrimitiveDoubleList)(reuse));
            array0 .clear();
        } else {
            array0 = new PrimitiveDoubleArrayList(((int) chunkLen0));
        }
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                array0 .addPrimitive((decoder.readDouble()));
            }
            chunkLen0 = (decoder.arrayNext());
        }
        return array0;
    }

}
