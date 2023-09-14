
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_5;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords_GenericDeserializer_963368136_963368136
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;
    private final Schema testOptionSchema0;
    private final Schema testOptionSchema1;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords_GenericDeserializer_963368136_963368136(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
        this.testOptionSchema0 = test0 .getTypes().get(0);
        this.testOptionSchema1 = test0 .getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords.put(0, deserializesubRecord20(FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords.get(0), (decoder)));
        } else {
            if (unionIndex0 == 1) {
                FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords.put(0, deserializesubRecord10(FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords.get(0), (decoder)));
            } else {
                if (unionIndex0 == 2) {
                    decoder.readNull();
                    FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords.put(0, null);
                } else {
                    throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
                }
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords;
    }

    public IndexedRecord deserializesubRecord20(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord2;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == testOptionSchema0)) {
            subRecord2 = ((IndexedRecord)(reuse));
        } else {
            subRecord2 = new org.apache.avro.generic.GenericData.Record(testOptionSchema0);
        }
        subRecord2 .put(0, (decoder.readInt()));
        return subRecord2;
    }

    public IndexedRecord deserializesubRecord10(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord1;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == testOptionSchema1)) {
            subRecord1 = ((IndexedRecord)(reuse));
        } else {
            subRecord1 = new org.apache.avro.generic.GenericData.Record(testOptionSchema1);
        }
        subRecord1 .put(0, (decoder.readInt()));
        return subRecord1;
    }

}
