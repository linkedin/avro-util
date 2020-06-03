
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_4;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion_GenericDeserializer_4313387729290221819_4313387729290221819
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema union875;
    private final Schema unionOptionSchema877;
    private final Schema subField879;

    public FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion_GenericDeserializer_4313387729290221819_4313387729290221819(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.union875 = readerSchema.getField("union").schema();
        this.unionOptionSchema877 = union875 .getTypes().get(1);
        this.subField879 = unionOptionSchema877 .getField("subField").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion874((reuse), (decoder));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion874(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion = ((IndexedRecord)(reuse));
        } else {
            FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex876 = (decoder.readIndex());
        if (unionIndex876 == 0) {
            decoder.readNull();
        }
        if (unionIndex876 == 1) {
            FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, deserializesubRecord878(FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.get(0), (decoder)));
        } else {
            if (unionIndex876 == 2) {
                if (FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.get(0) instanceof Utf8) {
                    FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, (decoder).readString(((Utf8) FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.get(0))));
                } else {
                    FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, (decoder).readString(null));
                }
            } else {
                if (unionIndex876 == 3) {
                    FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion.put(0, (decoder.readInt()));
                }
            }
        }
        return FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion;
    }

    public IndexedRecord deserializesubRecord878(Object reuse, Decoder decoder)
        throws IOException
    {
        IndexedRecord subRecord;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == unionOptionSchema877)) {
            subRecord = ((IndexedRecord)(reuse));
        } else {
            subRecord = new org.apache.avro.generic.GenericData.Record(unionOptionSchema877);
        }
        int unionIndex880 = (decoder.readIndex());
        if (unionIndex880 == 0) {
            decoder.readNull();
        }
        if (unionIndex880 == 1) {
            if (subRecord.get(0) instanceof Utf8) {
                subRecord.put(0, (decoder).readString(((Utf8) subRecord.get(0))));
            } else {
                subRecord.put(0, (decoder).readString(null));
            }
        }
        return subRecord;
    }

}
