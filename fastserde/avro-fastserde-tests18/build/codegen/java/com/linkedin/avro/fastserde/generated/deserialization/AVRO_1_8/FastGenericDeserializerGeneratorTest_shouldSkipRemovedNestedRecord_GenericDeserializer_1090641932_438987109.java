
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord_GenericDeserializer_1090641932_438987109
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema subRecord0;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord_GenericDeserializer_1090641932_438987109(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord0 = readerSchema.getField("subRecord").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord.put(0, deserializesubRecord0(FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord.get(0), (decoder)));
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord0)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecord0);
        }
        Object oldString0 = subRecord.get(0);
        if (oldString0 instanceof Utf8) {
            subRecord.put(0, (decoder).readString(((Utf8) oldString0)));
        } else {
            subRecord.put(0, (decoder).readString(null));
        }
        populate_subRecord0((subRecord), (decoder));
        populate_subRecord1((subRecord), (decoder));
        return subRecord;
    }

    private void populate_subRecord0(IndexedRecord subRecord, Decoder decoder)
        throws IOException
    {
        deserializesubSubRecord0(null, (decoder));
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                deserializesubSubRecord0(null, (decoder));
            } else {
                throw new RuntimeException(("Illegal union index for 'test3': "+ unionIndex0));
            }
        }
    }

    public void deserializesubSubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
        populate_subSubRecord0((decoder));
    }

    private void populate_subSubRecord0(Decoder decoder)
        throws IOException
    {
        decoder.skipString();
    }

    private void populate_subRecord1(IndexedRecord subRecord, Decoder decoder)
        throws IOException
    {
        Object oldString1 = subRecord.get(1);
        if (oldString1 instanceof Utf8) {
            subRecord.put(1, (decoder).readString(((Utf8) oldString1)));
        } else {
            subRecord.put(1, (decoder).readString(null));
        }
    }

}
