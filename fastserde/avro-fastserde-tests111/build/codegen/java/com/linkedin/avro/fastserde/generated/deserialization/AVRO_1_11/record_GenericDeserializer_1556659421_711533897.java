
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

public class record_GenericDeserializer_1556659421_711533897
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema subRecord0;

    public record_GenericDeserializer_1556659421_711533897(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.subRecord0 = readerSchema.getField("subRecord").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializerecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializerecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord record;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            record = ((IndexedRecord)(reuse));
        } else {
            record = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            throw new AvroTypeException("Found \"null\", expecting {\"type\":\"record\",\"name\":\"subRecord\",\"namespace\":\"com.linkedin.avro.fastserde.generated.avro\",\"doc\":\"subRecord\",\"fields\":[{\"name\":\"someInt1\",\"type\":\"int\",\"doc\":\"\"},{\"name\":\"someInt2\",\"type\":\"int\",\"doc\":\"\"}]}");
        } else {
            if (unionIndex0 == 1) {
                record.put(0, deserializesubRecord0(record.get(0), (decoder), (customization)));
            } else {
                throw new RuntimeException(("Illegal union index for 'subRecord': "+ unionIndex0));
            }
        }
        return record;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == subRecord0)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(subRecord0);
        }
        subRecord.put(0, (decoder.readInt()));
        populate_subRecord0((subRecord), (customization), (decoder));
        return subRecord;
    }

    private void populate_subRecord0(IndexedRecord subRecord, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        subRecord.put(1, (decoder.readInt()));
    }

}
