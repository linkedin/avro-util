
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class record_GenericDeserializer_711533897_1556659421
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema subRecord0;
    private final Schema subRecordRecordSchema0;

    public record_GenericDeserializer_711533897_1556659421(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord0 = readerSchema.getField("subRecord").schema();
        this.subRecordRecordSchema0 = subRecord0 .getTypes().get(1);
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializerecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            record0 = ((IndexedRecord)(reuse));
        } else {
            record0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        record0 .put(0, deserializesubRecord0(record0 .get(0), (decoder), (customization)));
        return record0;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord subRecord1;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecordRecordSchema0)) {
            subRecord1 = ((IndexedRecord)(reuse));
        } else {
            subRecord1 = new org.apache.avro.generic.GenericData.Record(subRecordRecordSchema0);
        }
        subRecord1 .put(0, (decoder.readInt()));
        populate_subRecord0((subRecord1), (customization), (decoder));
        return subRecord1;
    }

    private void populate_subRecord0(IndexedRecord subRecord1, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        subRecord1 .put(1, (decoder.readInt()));
    }

}
