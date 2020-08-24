
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
    private final Schema subRecord10;

    public FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord_GenericDeserializer_2934652623027673603_1427867983386866899(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord10 = readerSchema.getField("subRecord1").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord0((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.put(0, deserializesubRecord0(FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.get(0), (decoder)));
        deserializesubRecord20(null, (decoder));
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                deserializesubRecord20(null, (decoder));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'subRecord3': "+ unionIndex0));
        }
        FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.put(1, deserializesubRecord0(FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord.get(1), (decoder)));
        return FastGenericDeserializerGeneratorTest_shouldSkipRemovedRecord;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord10)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecord10);
        }
        Object oldString0 = subRecord.get(0);
        if (oldString0 instanceof Utf8) {
            subRecord.put(0, (decoder).readString(((Utf8) oldString0)));
        } else {
            subRecord.put(0, (decoder).readString(null));
        }
        Object oldString1 = subRecord.get(1);
        if (oldString1 instanceof Utf8) {
            subRecord.put(1, (decoder).readString(((Utf8) oldString1)));
        } else {
            subRecord.put(1, (decoder).readString(null));
        }
        return subRecord;
    }

    public void deserializesubRecord20(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
        decoder.skipString();
    }

}
