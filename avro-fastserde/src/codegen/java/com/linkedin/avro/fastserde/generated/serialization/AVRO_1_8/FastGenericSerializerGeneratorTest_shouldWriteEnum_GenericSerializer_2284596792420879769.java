
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_8;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteEnum_GenericSerializer_2284596792420879769
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteEnum68(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteEnum68(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeEnum(((org.apache.avro.generic.GenericData.EnumSymbol) data.get(0)).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) data.get(0)).toString()));
        org.apache.avro.generic.GenericData.EnumSymbol testEnumUnion69 = ((org.apache.avro.generic.GenericData.EnumSymbol) data.get(1));
        if (testEnumUnion69 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((testEnumUnion69 instanceof org.apache.avro.generic.GenericData.EnumSymbol)&&"com.adpilot.utils.generated.avro.testEnum".equals(((org.apache.avro.generic.GenericData.EnumSymbol) testEnumUnion69).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                (encoder).writeEnum(((org.apache.avro.generic.GenericData.EnumSymbol) testEnumUnion69).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) testEnumUnion69).toString()));
            }
        }
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumArray70 = ((List<org.apache.avro.generic.GenericData.EnumSymbol> ) data.get(2));
        (encoder).writeArrayStart();
        if ((testEnumArray70 == null)||testEnumArray70 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumArray70 .size());
            for (int counter71 = 0; (counter71 <((List<org.apache.avro.generic.GenericData.EnumSymbol> ) testEnumArray70).size()); counter71 ++) {
                (encoder).startItem();
                (encoder).writeEnum(((org.apache.avro.generic.GenericData.EnumSymbol) testEnumArray70 .get(counter71)).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) testEnumArray70 .get(counter71)).toString()));
            }
        }
        (encoder).writeArrayEnd();
        List<org.apache.avro.generic.GenericData.EnumSymbol> testEnumUnionArray72 = ((List<org.apache.avro.generic.GenericData.EnumSymbol> ) data.get(3));
        (encoder).writeArrayStart();
        if ((testEnumUnionArray72 == null)||testEnumUnionArray72 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumUnionArray72 .size());
            for (int counter73 = 0; (counter73 <((List<org.apache.avro.generic.GenericData.EnumSymbol> ) testEnumUnionArray72).size()); counter73 ++) {
                (encoder).startItem();
                org.apache.avro.generic.GenericData.EnumSymbol union74 = null;
                union74 = ((List<org.apache.avro.generic.GenericData.EnumSymbol> ) testEnumUnionArray72).get(counter73);
                if (union74 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union74 instanceof org.apache.avro.generic.GenericData.EnumSymbol)&&"com.adpilot.utils.generated.avro.testEnum".equals(((org.apache.avro.generic.GenericData.EnumSymbol) union74).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        (encoder).writeEnum(((org.apache.avro.generic.GenericData.EnumSymbol) union74).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) union74).toString()));
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
