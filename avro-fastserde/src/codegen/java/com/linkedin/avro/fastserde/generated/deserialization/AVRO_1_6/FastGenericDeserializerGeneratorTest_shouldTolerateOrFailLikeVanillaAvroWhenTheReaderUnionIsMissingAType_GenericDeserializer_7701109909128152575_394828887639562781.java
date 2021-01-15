
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType_GenericDeserializer_7701109909128152575_394828887639562781
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType_GenericDeserializer_7701109909128152575_394828887639562781(Schema readerSchema) {
        this.readerSchema = readerSchema;
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
            FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
            {
                Object oldString0 = FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType.get(0);
                if (oldString0 instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType.put(0, (decoder).readString(((Utf8) oldString0)));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType.put(0, (decoder).readString(null));
                }
                break;
            }
            case  2 :
                throw new AvroTypeException("Found \"long\", expecting [\"null\", \"string\"]");
            default:
                throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
        }
        return FastGenericDeserializerGeneratorTest_shouldTolerateOrFailLikeVanillaAvroWhenTheReaderUnionIsMissingAType;
    }

}
