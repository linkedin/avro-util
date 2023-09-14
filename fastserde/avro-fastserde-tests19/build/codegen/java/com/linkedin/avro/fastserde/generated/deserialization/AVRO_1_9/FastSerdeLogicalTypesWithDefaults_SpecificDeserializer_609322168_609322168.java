
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_9;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import com.linkedin.avro.api.PrimitiveIntList;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class FastSerdeLogicalTypesWithDefaults_SpecificDeserializer_609322168_609322168
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults>
{

    private final Schema readerSchema;

    public FastSerdeLogicalTypesWithDefaults_SpecificDeserializer_609322168_609322168(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults deserialize(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults reuse, Decoder decoder)
        throws IOException
    {
        return deserializeFastSerdeLogicalTypesWithDefaults0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults deserializeFastSerdeLogicalTypesWithDefaults0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults FastSerdeLogicalTypesWithDefaults;
        if ((reuse)!= null) {
            FastSerdeLogicalTypesWithDefaults = ((com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults)(reuse));
        } else {
            FastSerdeLogicalTypesWithDefaults = new com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults();
        }
        int unionIndex0 = (decoder.readIndex());
        if (unionIndex0 == 0) {
            PrimitiveIntList unionOfArrayAndMapOption0 = null;
            long chunkLen0 = (decoder.readArrayStart());
            Object oldArray0 = FastSerdeLogicalTypesWithDefaults.get(0);
            if (oldArray0 instanceof PrimitiveIntList) {
                unionOfArrayAndMapOption0 = ((PrimitiveIntList) oldArray0);
                unionOfArrayAndMapOption0 .clear();
            } else {
                unionOfArrayAndMapOption0 = new PrimitiveIntArrayList(((int) chunkLen0));
            }
            while (chunkLen0 > 0) {
                for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                    unionOfArrayAndMapOption0 .addPrimitive((decoder.readInt()));
                }
                chunkLen0 = (decoder.arrayNext());
            }
            FastSerdeLogicalTypesWithDefaults.put(0, unionOfArrayAndMapOption0);
        } else {
            if (unionIndex0 == 1) {
                Map<Utf8, Integer> unionOfArrayAndMapOption1 = null;
                long chunkLen1 = (decoder.readMapStart());
                if (chunkLen1 > 0) {
                    Map<Utf8, Integer> unionOfArrayAndMapOptionReuse0 = null;
                    Object oldMap0 = FastSerdeLogicalTypesWithDefaults.get(0);
                    if (oldMap0 instanceof Map) {
                        unionOfArrayAndMapOptionReuse0 = ((Map) oldMap0);
                    }
                    if (unionOfArrayAndMapOptionReuse0 != (null)) {
                        unionOfArrayAndMapOptionReuse0 .clear();
                        unionOfArrayAndMapOption1 = unionOfArrayAndMapOptionReuse0;
                    } else {
                        unionOfArrayAndMapOption1 = new HashMap<Utf8, Integer>(((int)(((chunkLen1 * 4)+ 2)/ 3)));
                    }
                    do {
                        for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                            Utf8 key0 = (decoder.readString(null));
                            unionOfArrayAndMapOption1 .put(key0, (decoder.readInt()));
                        }
                        chunkLen1 = (decoder.mapNext());
                    } while (chunkLen1 > 0);
                } else {
                    unionOfArrayAndMapOption1 = new HashMap<Utf8, Integer>(0);
                }
                FastSerdeLogicalTypesWithDefaults.put(0, unionOfArrayAndMapOption1);
            } else {
                throw new RuntimeException(("Illegal union index for 'unionOfArrayAndMap': "+ unionIndex0));
            }
        }
        populate_FastSerdeLogicalTypesWithDefaults0((FastSerdeLogicalTypesWithDefaults), (decoder));
        populate_FastSerdeLogicalTypesWithDefaults1((FastSerdeLogicalTypesWithDefaults), (decoder));
        populate_FastSerdeLogicalTypesWithDefaults2((FastSerdeLogicalTypesWithDefaults), (decoder));
        populate_FastSerdeLogicalTypesWithDefaults3((FastSerdeLogicalTypesWithDefaults), (decoder));
        populate_FastSerdeLogicalTypesWithDefaults4((FastSerdeLogicalTypesWithDefaults), (decoder));
        populate_FastSerdeLogicalTypesWithDefaults5((FastSerdeLogicalTypesWithDefaults), (decoder));
        return FastSerdeLogicalTypesWithDefaults;
    }

    private void populate_FastSerdeLogicalTypesWithDefaults0(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults FastSerdeLogicalTypesWithDefaults, Decoder decoder)
        throws IOException
    {
        Map<Utf8, Object> mapOfUnionsOfDateAndTimestampMillis0 = null;
        long chunkLen2 = (decoder.readMapStart());
        if (chunkLen2 > 0) {
            Map<Utf8, Object> mapOfUnionsOfDateAndTimestampMillisReuse0 = null;
            Object oldMap1 = FastSerdeLogicalTypesWithDefaults.get(1);
            if (oldMap1 instanceof Map) {
                mapOfUnionsOfDateAndTimestampMillisReuse0 = ((Map) oldMap1);
            }
            if (mapOfUnionsOfDateAndTimestampMillisReuse0 != (null)) {
                mapOfUnionsOfDateAndTimestampMillisReuse0 .clear();
                mapOfUnionsOfDateAndTimestampMillis0 = mapOfUnionsOfDateAndTimestampMillisReuse0;
            } else {
                mapOfUnionsOfDateAndTimestampMillis0 = new HashMap<Utf8, Object>(((int)(((chunkLen2 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                    Utf8 key1 = (decoder.readString(null));
                    int unionIndex1 = (decoder.readIndex());
                    if (unionIndex1 == 0) {
                        mapOfUnionsOfDateAndTimestampMillis0 .put(key1, (decoder.readInt()));
                    } else {
                        if (unionIndex1 == 1) {
                            mapOfUnionsOfDateAndTimestampMillis0 .put(key1, (decoder.readLong()));
                        } else {
                            throw new RuntimeException(("Illegal union index for 'mapOfUnionsOfDateAndTimestampMillisValue': "+ unionIndex1));
                        }
                    }
                }
                chunkLen2 = (decoder.mapNext());
            } while (chunkLen2 > 0);
        } else {
            mapOfUnionsOfDateAndTimestampMillis0 = new HashMap<Utf8, Object>(0);
        }
        FastSerdeLogicalTypesWithDefaults.put(1, mapOfUnionsOfDateAndTimestampMillis0);
        Map<Utf8, Long> timestampMillisMap0 = null;
        long chunkLen3 = (decoder.readMapStart());
        if (chunkLen3 > 0) {
            Map<Utf8, Long> timestampMillisMapReuse0 = null;
            Object oldMap2 = FastSerdeLogicalTypesWithDefaults.get(2);
            if (oldMap2 instanceof Map) {
                timestampMillisMapReuse0 = ((Map) oldMap2);
            }
            if (timestampMillisMapReuse0 != (null)) {
                timestampMillisMapReuse0 .clear();
                timestampMillisMap0 = timestampMillisMapReuse0;
            } else {
                timestampMillisMap0 = new HashMap<Utf8, Long>(((int)(((chunkLen3 * 4)+ 2)/ 3)));
            }
            do {
                for (int counter3 = 0; (counter3 <chunkLen3); counter3 ++) {
                    Utf8 key2 = (decoder.readString(null));
                    timestampMillisMap0 .put(key2, (decoder.readLong()));
                }
                chunkLen3 = (decoder.mapNext());
            } while (chunkLen3 > 0);
        } else {
            timestampMillisMap0 = new HashMap<Utf8, Long>(0);
        }
        FastSerdeLogicalTypesWithDefaults.put(2, timestampMillisMap0);
    }

    private void populate_FastSerdeLogicalTypesWithDefaults1(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults FastSerdeLogicalTypesWithDefaults, Decoder decoder)
        throws IOException
    {
        int unionIndex2 = (decoder.readIndex());
        if (unionIndex2 == 0) {
            decoder.readNull();
            FastSerdeLogicalTypesWithDefaults.put(3, null);
        } else {
            if (unionIndex2 == 1) {
                PrimitiveIntList nullableArrayOfDatesOption0 = null;
                long chunkLen4 = (decoder.readArrayStart());
                Object oldArray1 = FastSerdeLogicalTypesWithDefaults.get(3);
                if (oldArray1 instanceof PrimitiveIntList) {
                    nullableArrayOfDatesOption0 = ((PrimitiveIntList) oldArray1);
                    nullableArrayOfDatesOption0 .clear();
                } else {
                    nullableArrayOfDatesOption0 = new PrimitiveIntArrayList(((int) chunkLen4));
                }
                while (chunkLen4 > 0) {
                    for (int counter4 = 0; (counter4 <chunkLen4); counter4 ++) {
                        nullableArrayOfDatesOption0 .addPrimitive((decoder.readInt()));
                    }
                    chunkLen4 = (decoder.arrayNext());
                }
                FastSerdeLogicalTypesWithDefaults.put(3, nullableArrayOfDatesOption0);
            } else {
                throw new RuntimeException(("Illegal union index for 'nullableArrayOfDates': "+ unionIndex2));
            }
        }
        PrimitiveIntList arrayOfDates0 = null;
        long chunkLen5 = (decoder.readArrayStart());
        Object oldArray2 = FastSerdeLogicalTypesWithDefaults.get(4);
        if (oldArray2 instanceof PrimitiveIntList) {
            arrayOfDates0 = ((PrimitiveIntList) oldArray2);
            arrayOfDates0 .clear();
        } else {
            arrayOfDates0 = new PrimitiveIntArrayList(((int) chunkLen5));
        }
        while (chunkLen5 > 0) {
            for (int counter5 = 0; (counter5 <chunkLen5); counter5 ++) {
                arrayOfDates0 .addPrimitive((decoder.readInt()));
            }
            chunkLen5 = (decoder.arrayNext());
        }
        FastSerdeLogicalTypesWithDefaults.put(4, arrayOfDates0);
    }

    private void populate_FastSerdeLogicalTypesWithDefaults2(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults FastSerdeLogicalTypesWithDefaults, Decoder decoder)
        throws IOException
    {
        int unionIndex3 = (decoder.readIndex());
        if (unionIndex3 == 0) {
            Object oldBytes0 = FastSerdeLogicalTypesWithDefaults.get(5);
            if (oldBytes0 instanceof ByteBuffer) {
                FastSerdeLogicalTypesWithDefaults.put(5, (decoder).readBytes(((ByteBuffer) oldBytes0)));
            } else {
                FastSerdeLogicalTypesWithDefaults.put(5, (decoder).readBytes((null)));
            }
        } else {
            if (unionIndex3 == 1) {
                FastSerdeLogicalTypesWithDefaults.put(5, (decoder.readInt()));
            } else {
                throw new RuntimeException(("Illegal union index for 'unionOfDecimalOrDate': "+ unionIndex3));
            }
        }
        Object oldString0 = FastSerdeLogicalTypesWithDefaults.get(6);
        if (oldString0 instanceof Utf8) {
            FastSerdeLogicalTypesWithDefaults.put(6, (decoder).readString(((Utf8) oldString0)));
        } else {
            FastSerdeLogicalTypesWithDefaults.put(6, (decoder).readString(null));
        }
    }

    private void populate_FastSerdeLogicalTypesWithDefaults3(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults FastSerdeLogicalTypesWithDefaults, Decoder decoder)
        throws IOException
    {
        FastSerdeLogicalTypesWithDefaults.put(7, (decoder.readLong()));
        FastSerdeLogicalTypesWithDefaults.put(8, (decoder.readLong()));
    }

    private void populate_FastSerdeLogicalTypesWithDefaults4(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults FastSerdeLogicalTypesWithDefaults, Decoder decoder)
        throws IOException
    {
        FastSerdeLogicalTypesWithDefaults.put(9, (decoder.readInt()));
        FastSerdeLogicalTypesWithDefaults.put(10, (decoder.readLong()));
    }

    private void populate_FastSerdeLogicalTypesWithDefaults5(com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults FastSerdeLogicalTypesWithDefaults, Decoder decoder)
        throws IOException
    {
        FastSerdeLogicalTypesWithDefaults.put(11, (decoder.readInt()));
        FastSerdeLogicalTypesWithDefaults.put(12, deserializeLocalTimestampRecordWithDefaults0(FastSerdeLogicalTypesWithDefaults.get(12), (decoder)));
    }

    public com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults deserializeLocalTimestampRecordWithDefaults0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults LocalTimestampRecordWithDefaults;
        if ((reuse)!= null) {
            LocalTimestampRecordWithDefaults = ((com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults)(reuse));
        } else {
            LocalTimestampRecordWithDefaults = new com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults();
        }
        LocalTimestampRecordWithDefaults.put(0, (decoder.readLong()));
        populate_LocalTimestampRecordWithDefaults0((LocalTimestampRecordWithDefaults), (decoder));
        populate_LocalTimestampRecordWithDefaults1((LocalTimestampRecordWithDefaults), (decoder));
        return LocalTimestampRecordWithDefaults;
    }

    private void populate_LocalTimestampRecordWithDefaults0(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults LocalTimestampRecordWithDefaults, Decoder decoder)
        throws IOException
    {
        int unionIndex4 = (decoder.readIndex());
        if (unionIndex4 == 0) {
            decoder.readNull();
            LocalTimestampRecordWithDefaults.put(1, null);
        } else {
            if (unionIndex4 == 1) {
                LocalTimestampRecordWithDefaults.put(1, (decoder.readLong()));
            } else {
                throw new RuntimeException(("Illegal union index for 'nullableNestedTimestamp': "+ unionIndex4));
            }
        }
        int unionIndex5 = (decoder.readIndex());
        if (unionIndex5 == 0) {
            decoder.readNull();
            LocalTimestampRecordWithDefaults.put(2, null);
        } else {
            if (unionIndex5 == 1) {
                LocalTimestampRecordWithDefaults.put(2, (decoder.readInt()));
            } else {
                if (unionIndex5 == 2) {
                    LocalTimestampRecordWithDefaults.put(2, (decoder.readLong()));
                } else {
                    throw new RuntimeException(("Illegal union index for 'nullableUnionOfDateAndLocalTimestamp': "+ unionIndex5));
                }
            }
        }
    }

    private void populate_LocalTimestampRecordWithDefaults1(com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults LocalTimestampRecordWithDefaults, Decoder decoder)
        throws IOException
    {
        int unionIndex6 = (decoder.readIndex());
        if (unionIndex6 == 0) {
            LocalTimestampRecordWithDefaults.put(3, (decoder.readInt()));
        } else {
            if (unionIndex6 == 1) {
                LocalTimestampRecordWithDefaults.put(3, (decoder.readLong()));
            } else {
                throw new RuntimeException(("Illegal union index for 'unionOfDateAndLocalTimestamp': "+ unionIndex6));
            }
        }
    }

}
