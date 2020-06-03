
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_7;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion_GenericDeserializer_6068669851166793050_6068669851166793050
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema union729;
    private final Schema unionOptionSchema731;
    private final Schema subField733;

    public FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion_GenericDeserializer_6068669851166793050_6068669851166793050(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.union729 = readerSchema.getField("union").schema();
        this.unionOptionSchema731 = union729 .getTypes().get(1);
        this.subField733 = unionOptionSchema731 .getField("subField").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion728((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion728(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex730 = (decoder.readIndex());
        if (unionIndex730 == 0) {
            decoder.readNull();
        }
        if (unionIndex730 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, deserializesubRecord732(FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.get(0), (decoder)));
        } else {
            if (unionIndex730 == 2) {
                if (FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.get(0) instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.get(0))));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, (decoder).readString(null));
                }
            } else {
                if (unionIndex730 == 3) {
                    FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, (decoder.readInt()));
                }
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion;
    }

    public IndexedRecord deserializesubRecord732(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == unionOptionSchema731)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(unionOptionSchema731);
        }
        int unionIndex734 = (decoder.readIndex());
        if (unionIndex734 == 0) {
            decoder.readNull();
        }
        if (unionIndex734 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
