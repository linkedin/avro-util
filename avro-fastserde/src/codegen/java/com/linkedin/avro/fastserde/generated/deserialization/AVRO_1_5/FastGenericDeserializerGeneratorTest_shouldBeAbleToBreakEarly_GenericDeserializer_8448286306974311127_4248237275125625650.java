
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_5;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly_GenericDeserializer_8448286306974311127_4248237275125625650
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;

    public FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly_GenericDeserializer_8448286306974311127_4248237275125625650(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly.put(0, (decoder.readInt()));
        populate_FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly0((FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly), (decoder));
        return FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly;
    }

    private void populate_FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly0(IndexedRecord FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly, Decoder decoder)
        throws IOException
    {
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                decoder.readInt();
            } else {
                throw new RuntimeException(("Illegal union index for 'testIntUnion': "+ unionIndex0));
            }
        }
        Object oldString0 = FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly.get(1);
        if (oldString0 instanceof Utf8) {
            FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly.put(1, (decoder).readString(((Utf8) oldString0)));
        } else {
            FastGenericDeserializerGeneratorTest_shouldBeAbleToBreakEarly.put(1, (decoder).readString(null));
        }
    }

}
