
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.ByteBufferBackedPrimitiveFloatList;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

public class Array_of_FLOAT_GenericDeserializer_7282396011446356583_7282396011446356583
    implements FastDeserializer<List<Float>>
{

    private final Schema readerSchema;

    public Array_of_FLOAT_GenericDeserializer_7282396011446356583_7282396011446356583(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Float> deserialize(List<Float> reuse, Decoder decoder)
        throws IOException
    {
        List<Float> array602 = null;
        array602 = ((List<Float> ) ByteBufferBackedPrimitiveFloatList.readPrimitiveFloatArray((reuse), (decoder)));
        return array602;
    }

}
