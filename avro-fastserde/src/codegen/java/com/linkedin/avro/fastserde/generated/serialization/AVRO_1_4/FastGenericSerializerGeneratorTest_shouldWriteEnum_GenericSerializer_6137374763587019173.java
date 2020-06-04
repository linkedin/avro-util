
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_4;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteEnum_GenericSerializer_6137374763587019173
    implements FastSerializer<IndexedRecord>
{

    private final Schema testEnumEnumSchema69 = Schema.parse("{\"type\":\"enum\",\"name\":\"testEnum\",\"namespace\":\"com.adpilot.utils.generated.avro\",\"symbols\":[\"A\",\"B\"]}");

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteEnum68(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteEnum68(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeEnum(testEnumEnumSchema69 .getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) data.get(0)).toString()));
        org.apache.avro.generic.GenericData.EnumSymbol testEnumUnion70 = ((org.apache.avro.generic.GenericData.EnumSymbol) data.get(1));
        if (testEnumUnion70 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (testEnumUnion70 instanceof org.apache.avro.generic.GenericData.EnumSymbol) {
                (encoder).writeIndex(1);
                (encoder).writeEnum(testEnumEnumSchema69 .getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) testEnumUnion70).toString()));
            }
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray71 = ((List<org.apache.avro.generic.GenericData.EnumSymbol> ) data.get(2));
        (encoder).writeArrayStart();
        if ((testEnumArray71 == null)||testEnumArray71 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumArray71 .size());
            for (int counter72 = 0; (counter72 <((List<org.apache.avro.generic.GenericData.EnumSymbol> ) testEnumArray71).size()); counter72 ++) {
                (encoder).startItem();
                (encoder).writeEnum(testEnumEnumSchema69 .getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) testEnumArray71 .get(counter72)).toString()));
            }
        }
        (encoder).writeArrayEnd();
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray73 = ((List<org.apache.avro.generic.GenericData.EnumSymbol> ) data.get(3));
        (encoder).writeArrayStart();
        if ((testEnumUnionArray73 == null)||testEnumUnionArray73 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumUnionArray73 .size());
            for (int counter74 = 0; (counter74 <((List<org.apache.avro.generic.GenericData.EnumSymbol> ) testEnumUnionArray73).size()); counter74 ++) {
                (encoder).startItem();
                org.apache.avro.generic.GenericData.EnumSymbol union75 = null;
                union75 = ((List<org.apache.avro.generic.GenericData.EnumSymbol> ) testEnumUnionArray73).get(counter74);
                if (union75 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if (union75 instanceof org.apache.avro.generic.GenericData.EnumSymbol) {
                        (encoder).writeIndex(1);
                        (encoder).writeEnum(testEnumEnumSchema69 .getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) union75).toString()));
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
