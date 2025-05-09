
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.BufferBackedPrimitiveFloatList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
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

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeTestArrayOfFloats0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeTestArrayOfFloats0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord testArrayOfFloats0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            testArrayOfFloats0 = ((IndexedRecord)(reuse));
        } else {
            testArrayOfFloats0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        PrimitiveFloatList array_of_float1 = null;
        array_of_float1 = ((PrimitiveFloatList) BufferBackedPrimitiveFloatList.readPrimitiveFloatArray(testArrayOfFloats0 .get(0), (decoder)));
        testArrayOfFloats0 .put(0, array_of_float1);
        return testArrayOfFloats0;
    }

}
