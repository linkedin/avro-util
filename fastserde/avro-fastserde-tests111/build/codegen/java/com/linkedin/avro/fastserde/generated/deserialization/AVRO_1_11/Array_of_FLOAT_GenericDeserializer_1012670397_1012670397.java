
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

public class Array_of_FLOAT_GenericDeserializer_1012670397_1012670397
    implements FastDeserializer<List<Float>>
{

    private final Schema readerSchema;

    public Array_of_FLOAT_GenericDeserializer_1012670397_1012670397(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public List<Float> deserialize(List<Float> reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        PrimitiveFloatList array0 = null;
        array0 = ((PrimitiveFloatList) BufferBackedPrimitiveFloatList.readPrimitiveFloatArray((reuse), (decoder)));
        return array0;
    }

}
