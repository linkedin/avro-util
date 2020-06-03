
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class Array_of_LONG_GenericDeserializer_2055015354772118358_2055015354772118358
    implements FastDeserializer<List<Long>>
{

    private final Schema readerSchema;

    public Array_of_LONG_GenericDeserializer_2055015354772118358_2055015354772118358(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Long> deserialize(List<Long> reuse, Decoder decoder)
        throws IOException
    {
        List<Long> array754 = null;
        long chunkLen755 = (decoder.readArrayStart());
        if (chunkLen755 > 0) {
            List<Long> arrayReuse756 = null;
            if ((reuse) instanceof List) {
                arrayReuse756 = ((List)(reuse));
            }
            if (arrayReuse756 != (null)) {
                arrayReuse756 .clear();
                array754 = arrayReuse756;
            } else {
                array754 = new org.apache.avro.generic.GenericData.Array<Long>(((int) chunkLen755), readerSchema);
            }
            do {
                for (int counter757 = 0; (counter757 <chunkLen755); counter757 ++) {
                    Object arrayArrayElementReuseVar758 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar758 = ((GenericArray)(reuse)).peek();
                    }
                    array754 .add((decoder.readLong()));
                }
                chunkLen755 = (decoder.arrayNext());
            } while (chunkLen755 > 0);
        } else {
            array754 = new org.apache.avro.generic.GenericData.Array<Long>(0, readerSchema);
        }
        return array754;
    }

}
