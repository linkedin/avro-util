
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_6;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class RecordWithLargeUnionField_SpecificDeserializer_3405422059761328483_4069749625437031485
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField>
{

    private final Schema readerSchema;

    public RecordWithLargeUnionField_SpecificDeserializer_3405422059761328483_4069749625437031485(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField deserialize(com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField reuse, Decoder decoder)
        throws IOException
    {
        return deserializeRecordWithLargeUnionField0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField deserializeRecordWithLargeUnionField0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField RecordWithLargeUnionField;
        if ((reuse)!= null) {
            RecordWithLargeUnionField = ((com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField)(reuse));
        } else {
            RecordWithLargeUnionField = new com.linkedin.avro.fastserde.generated.avro.RecordWithLargeUnionField();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            Object oldString0 = RecordWithLargeUnionField.get(0);
            if (oldString0 instanceof Utf8) {
                RecordWithLargeUnionField.put(0, (decoder).readString(((Utf8) oldString0)));
            } else {
                RecordWithLargeUnionField.put(0, (decoder).readString(null));
            }
        } else {
            if (unionIndex0 == 1) {
                RecordWithLargeUnionField.put(0, (decoder.readInt()));
            } else {
                if (unionIndex0 == 2) {
                    throw new AvroTypeException(new StringBuilder().append("Found").append(" \"byt").append("es\", ").append("expec").append("ting ").append("[\"str").append("ing\",").append(" \"int").append("\"]").toString());
                } else {
                    throw new RuntimeException(("Illegal union index for 'unionField': "+ unionIndex0));
                }
            }
        }
        return RecordWithLargeUnionField;
    }

}
