
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_10;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.fastserde.FastSerializer;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;

public class FastSerdeEnums_GenericSerializer_957378572
    implements FastSerializer<IndexedRecord>
{


    public void serialize(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        serializeFastSerdeEnums0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeEnums0(IndexedRecord data, Encoder encoder)
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
        serialize_FastSerdeEnums0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeEnums0(IndexedRecord data, Encoder encoder)
        throws IOException
    {
        List<GenericEnumSymbol> arrayOfEnums0 = ((List<GenericEnumSymbol> ) data.get(1));
        if (arrayOfEnums0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (arrayOfEnums0 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<GenericEnumSymbol> ) arrayOfEnums0) == null)||((List<GenericEnumSymbol> ) arrayOfEnums0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<GenericEnumSymbol> ) arrayOfEnums0).size());
                    for (int counter0 = 0; (counter0 <((List<GenericEnumSymbol> ) arrayOfEnums0).size()); counter0 ++) {
                        (encoder).startItem();
                        int valueToWrite1;
                        Object enumValue1 = ((List<GenericEnumSymbol> ) arrayOfEnums0).get(counter0);
                        if (enumValue1 instanceof Enum) {
                            valueToWrite1 = ((Enum) enumValue1).ordinal();
                        } else {
                            valueToWrite1 = ((org.apache.avro.generic.GenericData.EnumSymbol) enumValue1).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) enumValue1).toString());
                        }
                        (encoder).writeEnum(valueToWrite1);
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        Map<CharSequence, GenericEnumSymbol> mapOfEnums0 = ((Map<CharSequence, GenericEnumSymbol> ) data.get(2));
        (encoder).writeMapStart();
        if ((mapOfEnums0 == null)||mapOfEnums0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(mapOfEnums0 .size());
            for (CharSequence key0 : ((Map<CharSequence, GenericEnumSymbol> ) mapOfEnums0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key0);
                int valueToWrite2;
                Object enumValue2 = mapOfEnums0 .get(key0);
                if (enumValue2 instanceof Enum) {
                    valueToWrite2 = ((Enum) enumValue2).ordinal();
                } else {
                    valueToWrite2 = ((org.apache.avro.generic.GenericData.EnumSymbol) enumValue2).getSchema().getEnumOrdinal(((org.apache.avro.generic.GenericData.EnumSymbol) enumValue2).toString());
                }
                (encoder).writeEnum(valueToWrite2);
            }
        }
        (encoder).writeMapEnd();
    }

}
