
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion_GenericDeserializer_2643982_2643982
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema union0;
    private final Schema unionOptionSchema0;
    private final Schema subField0;

    public FastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion_GenericDeserializer_2643982_2643982(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.union0 = readerSchema.getField("union").schema();
        this.unionOptionSchema0 = union0 .getTypes().get(1);
        this.subField0 = unionOptionSchema0 .getField("subField").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeFastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeFastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 = ((IndexedRecord)(reuse));
        } else {
            fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            decoder.readNull();
            fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 .put(0, null);
        } else {
            if (unionIndex0 == 1) {
                fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 .put(0, deserializesubRecord0(fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 .get(0), (decoder), (customization)));
            } else {
                if (unionIndex0 == 2) {
                    Utf8 charSequence1;
                    Object oldString1 = fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 .get(0);
                    if (oldString1 instanceof Utf8) {
                        charSequence1 = (decoder).readString(((Utf8) oldString1));
                    } else {
                        charSequence1 = (decoder).readString(null);
                    }
                    fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 .put(0, charSequence1);
                } else {
                    if (unionIndex0 == 3) {
                        fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0 .put(0, (decoder.readInt()));
                    } else {
                        throw new RuntimeException(("Illegal union index for 'union': "+ unionIndex0));
                    }
                }
            }
        }
        return fastGenericDeserializerGeneratorTest_shouldReadMultipleChoiceUnion0;
    }

    public IndexedRecord deserializesubRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord subRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == unionOptionSchema0)) {
            subRecord0 = ((IndexedRecord)(reuse));
        } else {
            subRecord0 = new org.apache.avro.generic.GenericData.Record(unionOptionSchema0);
        }
        int unionIndex1 = (decoder.readIndex());
        if (unionIndex1 == 0) {
            decoder.readNull();
            subRecord0 .put(0, null);
        } else {
            if (unionIndex1 == 1) {
                Utf8 charSequence0;
                Object oldString0 = subRecord0 .get(0);
                if (oldString0 instanceof Utf8) {
                    charSequence0 = (decoder).readString(((Utf8) oldString0));
                } else {
                    charSequence0 = (decoder).readString(null);
                }
                subRecord0 .put(0, charSequence0);
            } else {
                throw new RuntimeException(("Illegal union index for 'subField': "+ unionIndex1));
            }
        }
        return subRecord0;
    }

}
