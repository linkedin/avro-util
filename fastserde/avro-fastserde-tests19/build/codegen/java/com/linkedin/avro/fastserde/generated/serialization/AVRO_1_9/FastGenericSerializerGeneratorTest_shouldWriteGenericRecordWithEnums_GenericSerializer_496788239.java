
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_9;

import java.io.IOException;
import java.util.List;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithEnums_GenericSerializer_496788239
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithEnums0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithEnums0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        int valueToWrite0;
        Object enumValue0 = data.get(0);
        if (enumValue0 instanceof Enum) {
            valueToWrite0 = ((Enum) enumValue0).ordinal();
        } else {
            valueToWrite0 = ((org.apache.avro.generic.GenericData.EnumSymbol) enumValue0).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) enumValue0).toString());
        }
        (encoder).writeEnum(valueToWrite0);
        serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithEnums0(data, (encoder));
        serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithEnums1(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithEnums0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        GenericEnumSymbol testEnumUnion0 = ((GenericEnumSymbol) data.get(1));
        if (testEnumUnion0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if ((testEnumUnion0 instanceof GenericEnumSymbol)&&"com.linkedin.avro.fastserde.generated.avro.testEnum".equals(((GenericEnumSymbol) testEnumUnion0).getSchema().getFullName())) {
                (encoder).writeIndex(1);
                int valueToWrite1;
                Object enumValue1 = testEnumUnion0;
                if (enumValue1 instanceof Enum) {
                    valueToWrite1 = ((Enum) enumValue1).ordinal();
                } else {
                    valueToWrite1 = ((org.apache.avro.generic.GenericData.EnumSymbol) enumValue1).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) enumValue1).toString());
                }
                (encoder).writeEnum(valueToWrite1);
            }
        }
        List<GenericEnumSymbol> testEnumArray0 = ((List<GenericEnumSymbol> ) data.get(2));
        (encoder).writeArrayStart();
        if ((testEnumArray0 == null)||testEnumArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumArray0 .size());
            for (int counter0 = 0; (counter0 <testEnumArray0 .size()); counter0 ++) {
                (encoder).startItem();
                int valueToWrite2;
                Object enumValue2 = testEnumArray0 .get(counter0);
                if (enumValue2 instanceof Enum) {
                    valueToWrite2 = ((Enum) enumValue2).ordinal();
                } else {
                    valueToWrite2 = ((org.apache.avro.generic.GenericData.EnumSymbol) enumValue2).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) enumValue2).toString());
                }
                (encoder).writeEnum(valueToWrite2);
            }
        }
        (encoder).writeArrayEnd();
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastGenericSerializerGeneratorTest_shouldWriteGenericRecordWithEnums1(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<GenericEnumSymbol> testEnumUnionArray0 = ((List<GenericEnumSymbol> ) data.get(3));
        (encoder).writeArrayStart();
        if ((testEnumUnionArray0 == null)||testEnumUnionArray0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(testEnumUnionArray0 .size());
            for (int counter1 = 0; (counter1 <testEnumUnionArray0 .size()); counter1 ++) {
                (encoder).startItem();
                GenericEnumSymbol union0 = null;
                union0 = ((List<GenericEnumSymbol> ) testEnumUnionArray0).get(counter1);
                if (union0 == null) {
                    (encoder).writeIndex(0);
                    (encoder).writeNull();
                } else {
                    if ((union0 instanceof GenericEnumSymbol)&&"com.linkedin.avro.fastserde.generated.avro.testEnum".equals(((GenericEnumSymbol) union0).getSchema().getFullName())) {
                        (encoder).writeIndex(1);
                        int valueToWrite3;
                        Object enumValue3 = union0;
                        if (enumValue3 instanceof Enum) {
                            valueToWrite3 = ((Enum) enumValue3).ordinal();
                        } else {
                            valueToWrite3 = ((org.apache.avro.generic.GenericData.EnumSymbol) enumValue3).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) enumValue3).toString());
                        }
                        (encoder).writeEnum(valueToWrite3);
                    }
                }
            }
        }
        (encoder).writeArrayEnd();
    }

}
