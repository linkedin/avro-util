
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.generated.avro.MyEnumV1;
import com.linkedin.avro.fastserde.generated.avro.MyEnumV2;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

public class UnionOfRecordsWithSameNameEnumField_SpecificDeserializer_5893918162876115133_5893918162876115133
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField>
{

    private final Schema readerSchema;

    public UnionOfRecordsWithSameNameEnumField_SpecificDeserializer_5893918162876115133_5893918162876115133(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField deserialize(com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField reuse, Decoder decoder)
        throws IOException
    {
        return deserializeUnionOfRecordsWithSameNameEnumField0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField deserializeUnionOfRecordsWithSameNameEnumField0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField UnionOfRecordsWithSameNameEnumField;
        if ((reuse)!= null) {
            UnionOfRecordsWithSameNameEnumField = ((com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField)(reuse));
        } else {
            UnionOfRecordsWithSameNameEnumField = new com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            UnionOfRecordsWithSameNameEnumField.put(0, deserializeMyRecordV10(UnionOfRecordsWithSameNameEnumField.get(0), (decoder)));
        } else {
            if (unionIndex0 == 1) {
                UnionOfRecordsWithSameNameEnumField.put(0, deserializeMyRecordV20(UnionOfRecordsWithSameNameEnumField.get(0), (decoder)));
            } else {
                throw new RuntimeException(("Illegal union index for 'unionField': "+ unionIndex0));
            }
        }
        return UnionOfRecordsWithSameNameEnumField;
    }

    public com.linkedin.avro.fastserde.generated.avro.MyRecordV1 deserializeMyRecordV10(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.MyRecordV1 MyRecordV1;
        if ((reuse)!= null) {
            MyRecordV1 = ((com.linkedin.avro.fastserde.generated.avro.MyRecordV1)(reuse));
        } else {
            MyRecordV1 = new com.linkedin.avro.fastserde.generated.avro.MyRecordV1();
        }
        MyRecordV1 .put(0, MyEnumV1 .values()[(decoder.readEnum())]);
        return MyRecordV1;
    }

    public com.linkedin.avro.fastserde.generated.avro.MyRecordV2 deserializeMyRecordV20(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.MyRecordV2 MyRecordV2;
        if ((reuse)!= null) {
            MyRecordV2 = ((com.linkedin.avro.fastserde.generated.avro.MyRecordV2)(reuse));
        } else {
            MyRecordV2 = new com.linkedin.avro.fastserde.generated.avro.MyRecordV2();
        }
        MyRecordV2 .put(0, MyEnumV2 .values()[(decoder.readEnum())]);
        return MyRecordV2;
    }

}
