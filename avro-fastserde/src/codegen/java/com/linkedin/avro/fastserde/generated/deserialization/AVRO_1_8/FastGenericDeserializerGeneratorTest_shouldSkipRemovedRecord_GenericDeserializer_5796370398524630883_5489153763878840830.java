
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord_GenericDeserializer_5796370398524630883_5489153763878840830
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema subRecord1940;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord_GenericDeserializer_5796370398524630883_5489153763878840830(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord1940 = readerSchema.getField("subRecord1").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord939((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord939(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.put(0, deserializesubRecord941(FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.get(0), (decoder)));
        deserializesubRecord2942(null, (decoder));
        int unionIndex943 = (decoder.readIndex());
        if (unionIndex943 == 0) {
            decoder.readNull();
        }
        if (unionIndex943 == 1) {
            deserializesubRecord2942(null, (decoder));
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.put(1, deserializesubRecord941(FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.get(1), (decoder)));
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord;
    }

    public IndexedRecord deserializesubRecord941(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord1940)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecord1940);
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

    public void deserializesubRecord2942(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
        decoder.skipString();
    }

}
