
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString_GenericDeserializer_611693319_611693319
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString_GenericDeserializer_611693319_611693319(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString.put(0, null);
        } else {
            if (unionIndex0 == 1) {
                FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString.put(0, (decoder.readInt()));
            } else {
                if (unionIndex0 == 2) {
                    Object oldString0 = FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString.get(0);
                    if (oldString0 instanceof Utf8) {
                        FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString.put(0, (decoder).readString(((Utf8) oldString0)));
                    } else {
                        FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString.put(0, (decoder).readString(null));
                    }
                } else {
                    throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
                }
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingThatIncludeString;
    }

}
