
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType_GenericDeserializer_1186244769_367446918
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final GenericData modelData;
    private final Schema test0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType_GenericDeserializer_1186244769_367446918(Schema readerSchema, GenericData modelData) {
        this.readerSchema = readerSchema;
        this.modelData = modelData;
        this.test0 = readerSchema.getField("test").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType = new GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType.put(0, null);
        } else {
            if (unionIndex0 == 1) {
                Utf8 charSequence0;
                Object oldString0 = FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType.get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType.put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType;
    }

}
