
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import com.linkedin.avro.fastserde.generated.avro.MyEnumV1;
import com.linkedin.avro.fastserde.generated.avro.MyEnumV2;
import com.linkedin.avro.fastserde.generated.avro.MyRecordV1;
import com.linkedin.avro.fastserde.generated.avro.MyRecordV2;
import com.linkedin.avro.fastserde.generated.avro.UnionOfRecordsWithSameNameEnumField;
import com.linkedin.avroutil1.Enums;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;

public class UnionOfRecordsWithSameNameEnumField_SpecificDeserializer_1341667615_1341667615
    implements FastDeserializer<UnionOfRecordsWithSameNameEnumField>
{

    private final Schema readerSchema;

    public UnionOfRecordsWithSameNameEnumField_SpecificDeserializer_1341667615_1341667615(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public UnionOfRecordsWithSameNameEnumField deserialize(UnionOfRecordsWithSameNameEnumField reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeUnionOfRecordsWithSameNameEnumField0((reuse), (decoder), (customization));
    }

    public UnionOfRecordsWithSameNameEnumField deserializeUnionOfRecordsWithSameNameEnumField0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        UnionOfRecordsWithSameNameEnumField unionOfRecordsWithSameNameEnumField0;
        if ((reuse)!= null) {
            unionOfRecordsWithSameNameEnumField0 = ((UnionOfRecordsWithSameNameEnumField)(reuse));
        } else {
            unionOfRecordsWithSameNameEnumField0 = new UnionOfRecordsWithSameNameEnumField();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            unionOfRecordsWithSameNameEnumField0 .put(0, deserializeMyRecordV10(unionOfRecordsWithSameNameEnumField0 .get(0), (decoder), (customization)));
        } else {
            if (unionIndex0 == 1) {
                unionOfRecordsWithSameNameEnumField0 .put(0, deserializeMyRecordV20(unionOfRecordsWithSameNameEnumField0 .get(0), (decoder), (customization)));
            } else {
                throw new RuntimeException(("Illegal union index for 'unionField': "+ unionIndex0));
            }
        }
        return unionOfRecordsWithSameNameEnumField0;
    }

    public MyRecordV1 deserializeMyRecordV10(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        MyRecordV1 myRecordV10;
        if ((reuse)!= null) {
            myRecordV10 = ((MyRecordV1)(reuse));
        } else {
            myRecordV10 = new MyRecordV1();
        }
        myRecordV10 .put(0, Enums.getConstant(MyEnumV1 .class, (decoder.readEnum())));
        return myRecordV10;
    }

    public MyRecordV2 deserializeMyRecordV20(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        MyRecordV2 myRecordV20;
        if ((reuse)!= null) {
            myRecordV20 = ((MyRecordV2)(reuse));
        } else {
            myRecordV20 = new MyRecordV2();
        }
        myRecordV20 .put(0, Enums.getConstant(MyEnumV2 .class, (decoder.readEnum())));
        return myRecordV20;
    }

}
