
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
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

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        fastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0 .put(0, deserializesubRecord0(fastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0 .get(0), (decoder), (customization)));
        return fastGenericDeserializerGeneratorTest_shouldSkipRemovedNestedRecord0;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord subRecord1;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord0)) {
            subRecord1 = ((IndexedRecord)(reuse));
        } else {
            subRecord1 = new org.apache.avro.generic.GenericData.Record(subRecord0);
        }
        Utf8 charSequence0;
        Object oldString0 = subRecord1 .get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        subRecord1 .put(0, charSequence0);
        populate_subRecord0((subRecord1), (customization), (decoder));
        populate_subRecord1((subRecord1), (customization), (decoder));
        return subRecord1;
    }

    private void populate_subRecord0(IndexedRecord subRecord1, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        deserializesubSubRecord0(null, (decoder), (customization));
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
        } else {
            if (unionIndex0 == 1) {
                deserializesubSubRecord0(null, (decoder), (customization));
            } else {
                throw new RuntimeException(("Illegal union index for 'test3': "+ unionIndex0));
            }
        }
    }

    public void deserializesubSubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        decoder.skipString();
        populate_subSubRecord0((customization), (decoder));
    }

    private void populate_subSubRecord0(DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
    }

    private void populate_subRecord1(IndexedRecord subRecord1, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        Utf8 charSequence1;
        Object oldString1 = subRecord1 .get(1);
        if (oldString1 instanceof Utf8) {
            charSequence1 = (decoder).readString(((Utf8) oldString1));
        } else {
            charSequence1 = (decoder).readString(null);
        }
        subRecord1 .put(1, charSequence1);
    }

}
