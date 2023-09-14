
package com.linkedin.avro.fastserde.generated.serialization.AVRO_1_11;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults;
import com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

public class FastSerdeLogicalTypesWithDefaults_SpecificSerializer_609322168
    implements FastSerializer<FastSerdeLogicalTypesWithDefaults>
{


    public void serialize(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        serializeFastSerdeLogicalTypesWithDefaults0(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeFastSerdeLogicalTypesWithDefaults0(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object unionOfArrayAndMap0 = ((Object) data.get(0));
        if (unionOfArrayAndMap0 instanceof List) {
            (encoder).writeIndex(0);
            (encoder).writeArrayStart();
            if ((((List<Integer> ) unionOfArrayAndMap0) == null)||((List<Integer> ) unionOfArrayAndMap0).isEmpty()) {
                (encoder).setItemCount(0);
            } else {
                (encoder).setItemCount(((List<Integer> ) unionOfArrayAndMap0).size());
                if (((List<Integer> ) unionOfArrayAndMap0) instanceof PrimitiveIntList) {
                    PrimitiveIntList primitiveList0 = null;
                    primitiveList0 = ((PrimitiveIntList)((List<Integer> ) unionOfArrayAndMap0));
                    for (int counter0 = 0; (counter0 <primitiveList0 .size()); counter0 ++) {
                        (encoder).startItem();
                        (encoder).writeInt(primitiveList0 .getPrimitive(counter0));
                    }
                } else {
                    for (int counter1 = 0; (counter1 <((List<Integer> ) unionOfArrayAndMap0).size()); counter1 ++) {
                        (encoder).startItem();
                        (encoder).writeInt(((List<Integer> ) unionOfArrayAndMap0).get(counter1));
                    }
                }
            }
            (encoder).writeArrayEnd();
        } else {
            if (unionOfArrayAndMap0 instanceof Map) {
                (encoder).writeIndex(1);
                (encoder).writeMapStart();
                if ((((Map<CharSequence, Integer> ) unionOfArrayAndMap0) == null)||((Map<CharSequence, Integer> ) unionOfArrayAndMap0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((Map<CharSequence, Integer> ) unionOfArrayAndMap0).size());
                    for (CharSequence key0 : ((Map<CharSequence, Integer> )((Map<CharSequence, Integer> ) unionOfArrayAndMap0)).keySet()) {
                        (encoder).startItem();
                        (encoder).writeString(key0);
                        (encoder).writeInt(((Integer)((Map<CharSequence, Integer> ) unionOfArrayAndMap0).get(key0)));
                    }
                }
                (encoder).writeMapEnd();
            }
        }
        serialize_FastSerdeLogicalTypesWithDefaults0(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults1(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults2(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults3(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults4(data, (encoder));
        serialize_FastSerdeLogicalTypesWithDefaults5(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults0(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Map<CharSequence, Object> mapOfUnionsOfDateAndTimestampMillis0 = ((Map<CharSequence, Object> ) data.get(1));
        (encoder).writeMapStart();
        if ((mapOfUnionsOfDateAndTimestampMillis0 == null)||mapOfUnionsOfDateAndTimestampMillis0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(mapOfUnionsOfDateAndTimestampMillis0 .size());
            for (CharSequence key1 : ((Map<CharSequence, Object> ) mapOfUnionsOfDateAndTimestampMillis0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key1);
                Object union0 = null;
                union0 = ((Map<CharSequence, Object> ) mapOfUnionsOfDateAndTimestampMillis0).get(key1);
                if (union0 instanceof Integer) {
                    (encoder).writeIndex(0);
                    (encoder).writeInt(((Integer) union0));
                } else {
                    if (union0 instanceof Long) {
                        (encoder).writeIndex(1);
                        (encoder).writeLong(((Long) union0));
                    }
                }
            }
        }
        (encoder).writeMapEnd();
        Map<CharSequence, Long> timestampMillisMap0 = ((Map<CharSequence, Long> ) data.get(2));
        (encoder).writeMapStart();
        if ((timestampMillisMap0 == null)||timestampMillisMap0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(timestampMillisMap0 .size());
            for (CharSequence key2 : ((Map<CharSequence, Long> ) timestampMillisMap0).keySet()) {
                (encoder).startItem();
                (encoder).writeString(key2);
                (encoder).writeLong(((Long) timestampMillisMap0 .get(key2)));
            }
        }
        (encoder).writeMapEnd();
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults1(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        List<Integer> nullableArrayOfDates0 = ((List<Integer> ) data.get(3));
        if (nullableArrayOfDates0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (nullableArrayOfDates0 instanceof List) {
                (encoder).writeIndex(1);
                (encoder).writeArrayStart();
                if ((((List<Integer> ) nullableArrayOfDates0) == null)||((List<Integer> ) nullableArrayOfDates0).isEmpty()) {
                    (encoder).setItemCount(0);
                } else {
                    (encoder).setItemCount(((List<Integer> ) nullableArrayOfDates0).size());
                    if (((List<Integer> ) nullableArrayOfDates0) instanceof PrimitiveIntList) {
                        PrimitiveIntList primitiveList1 = null;
                        primitiveList1 = ((PrimitiveIntList)((List<Integer> ) nullableArrayOfDates0));
                        for (int counter2 = 0; (counter2 <primitiveList1 .size()); counter2 ++) {
                            (encoder).startItem();
                            (encoder).writeInt(primitiveList1 .getPrimitive(counter2));
                        }
                    } else {
                        for (int counter3 = 0; (counter3 <((List<Integer> ) nullableArrayOfDates0).size()); counter3 ++) {
                            (encoder).startItem();
                            (encoder).writeInt(((List<Integer> ) nullableArrayOfDates0).get(counter3));
                        }
                    }
                }
                (encoder).writeArrayEnd();
            }
        }
        List<Integer> arrayOfDates0 = ((List<Integer> ) data.get(4));
        (encoder).writeArrayStart();
        if ((arrayOfDates0 == null)||arrayOfDates0 .isEmpty()) {
            (encoder).setItemCount(0);
        } else {
            (encoder).setItemCount(arrayOfDates0 .size());
            if (arrayOfDates0 instanceof PrimitiveIntList) {
                PrimitiveIntList primitiveList2 = null;
                primitiveList2 = ((PrimitiveIntList) arrayOfDates0);
                for (int counter4 = 0; (counter4 <primitiveList2 .size()); counter4 ++) {
                    (encoder).startItem();
                    (encoder).writeInt(primitiveList2 .getPrimitive(counter4));
                }
            } else {
                for (int counter5 = 0; (counter5 <arrayOfDates0 .size()); counter5 ++) {
                    (encoder).startItem();
                    (encoder).writeInt(arrayOfDates0 .get(counter5));
                }
            }
        }
        (encoder).writeArrayEnd();
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults2(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object unionOfDecimalOrDate0 = ((Object) data.get(5));
        if (unionOfDecimalOrDate0 instanceof ByteBuffer) {
            (encoder).writeIndex(0);
            (encoder).writeBytes(((ByteBuffer) unionOfDecimalOrDate0));
        } else {
            if (unionOfDecimalOrDate0 instanceof Integer) {
                (encoder).writeIndex(1);
                (encoder).writeInt(((Integer) unionOfDecimalOrDate0));
            }
        }
        if (data.get(6) instanceof Utf8) {
            (encoder).writeString(((Utf8) data.get(6)));
        } else {
            (encoder).writeString(data.get(6).toString());
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults3(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        (encoder).writeLong(((Long) data.get(7)));
        (encoder).writeLong(((Long) data.get(8)));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults4(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        (encoder).writeInt(((Integer) data.get(9)));
        (encoder).writeLong(((Long) data.get(10)));
    }

    @SuppressWarnings("unchecked")
    private void serialize_FastSerdeLogicalTypesWithDefaults5(FastSerdeLogicalTypesWithDefaults data, Encoder encoder)
        throws IOException
    {
        (encoder).writeInt(((Integer) data.get(11)));
        LocalTimestampRecordWithDefaults nestedLocalTimestampMillis0 = ((LocalTimestampRecordWithDefaults) data.get(12));
        serializeLocalTimestampRecordWithDefaults0(nestedLocalTimestampMillis0, (encoder));
    }

    @SuppressWarnings("unchecked")
    public void serializeLocalTimestampRecordWithDefaults0(LocalTimestampRecordWithDefaults data, Encoder encoder)
        throws IOException
    {
        (encoder).writeLong(((Long) data.get(0)));
        serialize_LocalTimestampRecordWithDefaults0(data, (encoder));
        serialize_LocalTimestampRecordWithDefaults1(data, (encoder));
    }

    @SuppressWarnings("unchecked")
    private void serialize_LocalTimestampRecordWithDefaults0(LocalTimestampRecordWithDefaults data, Encoder encoder)
        throws IOException
    {
        Long nullableNestedTimestamp0 = ((Long) data.get(1));
        if (nullableNestedTimestamp0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            (encoder).writeIndex(1);
            (encoder).writeLong(((Long) nullableNestedTimestamp0));
        }
        Object nullableUnionOfDateAndLocalTimestamp0 = ((Object) data.get(2));
        if (nullableUnionOfDateAndLocalTimestamp0 == null) {
            (encoder).writeIndex(0);
            (encoder).writeNull();
        } else {
            if (nullableUnionOfDateAndLocalTimestamp0 instanceof Integer) {
                (encoder).writeIndex(1);
                (encoder).writeInt(((Integer) nullableUnionOfDateAndLocalTimestamp0));
            } else {
                if (nullableUnionOfDateAndLocalTimestamp0 instanceof Long) {
                    (encoder).writeIndex(2);
                    (encoder).writeLong(((Long) nullableUnionOfDateAndLocalTimestamp0));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void serialize_LocalTimestampRecordWithDefaults1(LocalTimestampRecordWithDefaults data, Encoder encoder)
        throws IOException
    {
        Object unionOfDateAndLocalTimestamp0 = ((Object) data.get(3));
        if (unionOfDateAndLocalTimestamp0 instanceof Integer) {
            (encoder).writeIndex(0);
            (encoder).writeInt(((Integer) unionOfDateAndLocalTimestamp0));
        } else {
            if (unionOfDateAndLocalTimestamp0 instanceof Long) {
                (encoder).writeIndex(1);
                (encoder).writeLong(((Long) unionOfDateAndLocalTimestamp0));
            }
        }
    }

}
