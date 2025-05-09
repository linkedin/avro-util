
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString_GenericDeserializer_1986844009_611693319
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString_GenericDeserializer_1986844009_611693319(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0 .put(0, charSequence0);
            } else {
                if (unionIndex0 == 2) {
                    fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0 .put(0, (decoder.readInt()));
                } else {
                    throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
                }
            }
        }
        return fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0;
    }

}
