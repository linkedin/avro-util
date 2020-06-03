
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord_GenericDeserializer_1758853045511797997_8691597471955696565
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema subRecord1081;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord_GenericDeserializer_1758853045511797997_8691597471955696565(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord1081 = readerSchema.getField("subRecord").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord1080((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord1080(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord.put(0, deserializesubRecord1082(FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord.get(0), (decoder)));
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord;
    }

    public IndexedRecord deserializesubRecord1082(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord1081)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecord1081);
        }
        if (subRecord.get(0) instanceof Utf8) {
            subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
        } else {
            subRecord.put(0, (decoder).readString(null));
        }
        deserializesubSubRecord1083(null, (decoder));
        int unionIndex1084 = (decoder.readIndex());
        if (unionIndex1084 == 0) {
            decoder.readNull();
        }
        if (unionIndex1084 == 1) {
            deserializesubSubRecord1083(null, (decoder));
        }
        if (subRecord.get(1) instanceof Utf8) {
            subRecord.put(1, (decoder).readString(((Utf8) subRecord.get(1))));
        } else {
            subRecord.put(1, (decoder).readString(null));
        }
        return subRecord;
    }

    public void deserializesubSubRecord1083(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
        decoder.skipString();
    }

}
