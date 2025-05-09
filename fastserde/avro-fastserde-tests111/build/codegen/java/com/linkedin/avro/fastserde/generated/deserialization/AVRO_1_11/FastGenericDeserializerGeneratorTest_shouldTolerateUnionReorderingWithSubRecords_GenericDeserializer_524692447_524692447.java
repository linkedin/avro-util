
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords_GenericDeserializer_524692447_524692447
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema test0;
    private final Schema testOptionSchema0;
    private final Schema testOptionSchema1;

    public FastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords_GenericDeserializer_524692447_524692447(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.test0 = readerSchema.getField("test").schema();
        this.testOptionSchema0 = test0 .getTypes().get(1);
        this.testOptionSchema1 = test0 .getTypes().get(2);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0 .put(0, deserializesubRecord10(fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0 .get(0), (decoder), (customization)));
            } else {
                if (unionIndex0 == 2) {
                    fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0 .put(0, deserializesubRecord20(fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0 .get(0), (decoder), (customization)));
                } else {
                    throw new RuntimeException(("Illegal union index for 'test': "+ unionIndex0));
                }
            }
        }
        return fastGenericDeserializerGeneratorTest_shouldTolerateUnionReorderingWithSubRecords0;
    }

    public IndexedRecord deserializesubRecord10(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord subRecord10;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == testOptionSchema0)) {
            subRecord10 = ((IndexedRecord)(reuse));
        } else {
            subRecord10 = new org.apache.avro.generic.GenericData.Record(testOptionSchema0);
        }
        subRecord10 .put(0, (decoder.readInt()));
        return subRecord10;
    }

    public IndexedRecord deserializesubRecord20(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord subRecord20;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == testOptionSchema1)) {
            subRecord20 = ((IndexedRecord)(reuse));
        } else {
            subRecord20 = new org.apache.avro.generic.GenericData.Record(testOptionSchema1);
        }
        subRecord20 .put(0, (decoder.readInt()));
        return subRecord20;
    }

}
