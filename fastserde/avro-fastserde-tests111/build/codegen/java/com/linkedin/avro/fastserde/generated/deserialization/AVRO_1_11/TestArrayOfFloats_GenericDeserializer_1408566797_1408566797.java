
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class TestArrayOfFloats_GenericDeserializer_1408566797_1408566797
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema array_of_float0;

    public TestArrayOfFloats_GenericDeserializer_1408566797_1408566797(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.array_of_float0 = readerSchema.getField("array_of_float").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeTestArrayOfFloats0((reuse), (decoder));
    }

    public IndexedRecord deserializeTestArrayOfFloats0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord TestArrayOfFloats;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            TestArrayOfFloats = ((IndexedRecord)(reuse));
        } else {
            TestArrayOfFloats = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        PrimitiveFloatList array_of_float1 = null;
        array_of_float1 = ((PrimitiveFloatList) BufferBackedPrimitiveFloatList.readPrimitiveFloatArray(TestArrayOfFloats.get(0), (decoder)));
        TestArrayOfFloats.put(0, array_of_float1);
        return TestArrayOfFloats;
    }

}
