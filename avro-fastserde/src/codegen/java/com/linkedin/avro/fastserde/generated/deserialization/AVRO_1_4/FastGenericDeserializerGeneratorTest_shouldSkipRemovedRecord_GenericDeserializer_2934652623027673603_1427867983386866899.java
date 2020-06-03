
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord_GenericDeserializer_2934652623027673603_1427867983386866899
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema subRecord11086;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord_GenericDeserializer_2934652623027673603_1427867983386866899(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord11086 = readerSchema.getField("subRecord1").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord1085((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord1085(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.put(0, deserializesubRecord1087(FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.get(0), (decoder)));
        deserializesubRecord21088(null, (decoder));
        int unionIndex1089 = (decoder.readIndex());
        if (unionIndex1089 == 0) {
            decoder.readNull();
        }
        if (unionIndex1089 == 1) {
            deserializesubRecord21088(null, (decoder));
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.put(1, deserializesubRecord1087(FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.get(1), (decoder)));
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord;
    }

    public IndexedRecord deserializesubRecord1087(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord11086)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecord11086);
        }
        if (subRecord.get(0) instanceof Utf8) {
            subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
        } else {
            subRecord.put(0, (decoder).readString(null));
        }
        if (subRecord.get(1) instanceof Utf8) {
            subRecord.put(1, (decoder).readString(((Utf8) subRecord.get(1))));
        } else {
            subRecord.put(1, (decoder).readString(null));
        }
        return subRecord;
    }

    public void deserializesubRecord21088(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
        decoder.skipString();
    }

}
