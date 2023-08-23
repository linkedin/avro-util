
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_10;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteEnum_GenericSerializer_1821537685
    implements FastSerializer<IndexedRecord>
{

    private final GenericData modelData;

    public FastGenericSerializerGeneratorTest_shouldWriteEnum_GenericSerializer_1821537685(GenericData modelData) {
        this.modelData = modelData;
    }

    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteEnum0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteEnum0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        (encoder).writeEnum(((GenericData.EnumSymbol) data.get(0)).getSchema().getEnumOrdinal(((GenericData.EnumSymbol) data.get(0)).toString()));
        GenericData.EnumSymbol testEnumUnion0 = ((GenericData.EnumSymbol) data.get(1));
        if (testEnumUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((testEnumUnion0 instanceof GenericData.EnumSymbol)&&"com.adpilot.utils.generated.avro.testEnum".equals(((GenericData.EnumSymbol) testEnumUnion0).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                (encoder).writeEnum(((GenericData.EnumSymbol) testEnumUnion0).getSchema().getEnumOrdinal(((GenericData.EnumSymbol) testEnumUnion0).toString()));
            }
        }
        List<GenericData.EnumSymbol> testEnumArray0 = ((List<GenericData.EnumSymbol> ) data.get(2));
        (encoder).writeArrayStart();
        Object array0 = testEnumArray0;
        if ((testEnumArray0 == null)||testEnumArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumArray0 .size());
            for (int counter0 = 0; (counter0 <testEnumArray0 .size()); counter0 ++) {
                (encoder).startItem();
                (encoder).writeEnum(((GenericData.EnumSymbol) testEnumArray0 .get(counter0)).getSchema().getEnumOrdinal(((GenericData.EnumSymbol) testEnumArray0 .get(counter0)).toString()));
            }
        }
        (encoder).writeArrayEnd();
        List<GenericData.EnumSymbol> testEnumUnionArray0 = ((List<GenericData.EnumSymbol> ) data.get(3));
        (encoder).writeArrayStart();
        Object array1 = testEnumUnionArray0;
        if ((testEnumUnionArray0 == null)||testEnumUnionArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumUnionArray0 .size());
            for (int counter1 = 0; (counter1 <testEnumUnionArray0 .size()); counter1 ++) {
                (encoder).startItem();
                GenericData.EnumSymbol union0 = null;
                union0 = ((List<GenericData.EnumSymbol> ) testEnumUnionArray0).get(counter1);
                if (union0 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union0 instanceof GenericData.EnumSymbol)&&"com.adpilot.utils.generated.avro.testEnum".equals(((GenericData.EnumSymbol) union0).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        (encoder).writeEnum(((GenericData.EnumSymbol) union0).getSchema().getEnumOrdinal(((GenericData.EnumSymbol) union0).toString()));
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
