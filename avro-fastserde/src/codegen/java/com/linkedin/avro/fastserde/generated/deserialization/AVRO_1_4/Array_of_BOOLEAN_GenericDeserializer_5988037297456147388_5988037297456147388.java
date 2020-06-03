
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;

public class Array_of_BOOLEAN_GenericDeserializer_5988037297456147388_5988037297456147388
    implements FastDeserializer<List<Boolean>>
{

    private final Schema readerSchema;

    public Array_of_BOOLEAN_GenericDeserializer_5988037297456147388_5988037297456147388(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Boolean> deserialize(List<Boolean> reuse, Decoder decoder)
        throws IOException
    {
        List<Boolean> array738 = null;
        long chunkLen739 = (decoder.readArrayStart());
        if (chunkLen739 > 0) {
            List<Boolean> arrayReuse740 = null;
            if ((reuse) instanceof List) {
                arrayReuse740 = ((List)(reuse));
            }
            if (arrayReuse740 != (null)) {
                arrayReuse740 .clear();
                array738 = arrayReuse740;
            } else {
                array738 = new org.apache.avro.generic.GenericData.Array<Boolean>(((int) chunkLen739), readerSchema);
            }
            do {
                for (int counter741 = 0; (counter741 <chunkLen739); counter741 ++) {
                    Object arrayArrayElementReuseVar742 = null;
                    if ((reuse) instanceof GenericArray) {
                        arrayArrayElementReuseVar742 = ((GenericArray)(reuse)).peek();
                    }
                    array738 .add((decoder.readBoolean()));
                }
                chunkLen739 = (decoder.arrayNext());
            } while (chunkLen739 > 0);
        } else {
            array738 = new org.apache.avro.generic.GenericData.Array<Boolean>(0, readerSchema);
        }
        return array738;
    }

}
