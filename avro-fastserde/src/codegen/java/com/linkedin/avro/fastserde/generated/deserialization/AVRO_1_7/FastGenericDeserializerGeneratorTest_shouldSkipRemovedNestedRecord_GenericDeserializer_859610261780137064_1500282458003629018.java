
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord_GenericDeserializer_859610261780137064_1500282458003629018
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema subRecord935;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord_GenericDeserializer_859610261780137064_1500282458003629018(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord935 = readerSchema.getField("subRecord").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord934((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord934(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord.put(0, deserializesubRecord936(FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord.get(0), (decoder)));
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord;
    }

    public IndexedRecord deserializesubRecord936(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord935)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecord935);
        }
        if (subRecord.get(0) instanceof Utf8) {
            subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
        } else {
            subRecord.put(0, (decoder).readString(null));
        }
        deserializesubSubRecord937(null, (decoder));
        int unionIndex938 = (decoder.readIndex());
        if (unionIndex938 == 0) {
            decoder.readNull();
        }
        if (unionIndex938 == 1) {
            deserializesubSubRecord937(null, (decoder));
        }
        if (subRecord.get(1) instanceof Utf8) {
            subRecord.put(1, (decoder).readString(((Utf8) subRecord.get(1))));
        } else {
            subRecord.put(1, (decoder).readString(null));
        }
        return subRecord;
    }

    public void deserializesubSubRecord937(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
        decoder.skipString();
    }

}
