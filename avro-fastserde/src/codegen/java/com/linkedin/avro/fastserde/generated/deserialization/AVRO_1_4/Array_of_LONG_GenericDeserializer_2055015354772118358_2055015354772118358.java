
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
        List<Long> array0 = null;
        long chunkLen0 = (decoder.readArrayStart());
        if (chunkLen0 > 0) {
            List<Long> arrayReuse0 = null;
            if ((reuse) instanceof List) {
                arrayReuse0 = ((List)(reuse));
            }
            if (arrayReuse0 != (null)) {
                arrayReuse0 .clear();
                array0 = arrayReuse0;
            } else {
                array0 = new org.apache.avro.generic.GenericData.Array<Long>(((int) chunkLen0), readerSchema);
            }
            do {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    Object arrayArrayElementReuseVar0 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar0 = ((GenericArray)(reuse)).peek();
                    }
                    array0 .add((decoder.readLong()));
                }
                chunkLen0 = (decoder.arrayNext());
            } while (chunkLen0 > 0);
        } else {
            array0 = new org.apache.avro.generic.GenericData.Array<Long>(0, readerSchema);
        }
        return array0;
    }

}
