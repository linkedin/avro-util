
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString_GenericDeserializer_616931169_616931169
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString_GenericDeserializer_616931169_616931169(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0 .put(0, (decoder.readInt()));
            } else {
                if (unionIndex0 == 2) {
                    fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0 .put(0, (decoder.readBoolean()));
                } else {
                    throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
                }
            }
        }
        return fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithNonString0;
    }

}
