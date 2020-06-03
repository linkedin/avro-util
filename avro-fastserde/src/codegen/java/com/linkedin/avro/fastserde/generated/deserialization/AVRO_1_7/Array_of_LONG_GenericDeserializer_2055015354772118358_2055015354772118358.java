
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

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
        List<Long> array608 = null;
        long chunkLen609 = (decoder.readArrayStart());
        if (chunkLen609 > 0) {
            List<Long> arrayReuse610 = null;
            if ((reuse) instanceof List) {
                arrayReuse610 = ((List)(reuse));
            }
            if (arrayReuse610 != (null)) {
                arrayReuse610 .clear();
                array608 = arrayReuse610;
            } else {
                array608 = new org.apache.avro.generic.GenericData.Array<Long>(((int) chunkLen609), readerSchema);
            }
            do {
                for (int counter611 = 0; (counter611 <chunkLen609); counter611 ++) {
                    Object arrayArrayElementReuseVar612 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar612 = ((GenericArray)(reuse)).peek();
                    }
                    array608 .add((decoder.readLong()));
                }
                chunkLen609 = (decoder.arrayNext());
            } while (chunkLen609 > 0);
        } else {
            array608 = new org.apache.avro.generic.GenericData.Array<Long>(0, readerSchema);
        }
        return array608;
    }

}
