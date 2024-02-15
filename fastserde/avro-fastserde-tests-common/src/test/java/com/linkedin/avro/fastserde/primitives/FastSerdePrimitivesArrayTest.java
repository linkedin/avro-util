package com.linkedin.avro.fastserde.primitives;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.getField;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.linkedin.avro.api.PrimitiveFloatList;
import com.linkedin.avro.fastserde.FastSerdeTestsSupport;
import com.linkedin.avro.fastserde.FastSpecificDatumReader;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveBooleanList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveDoubleList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveFloatList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveIntList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveLongList;
import com.linkedin.avro.fastserde.generated.avro.PrimitiveArraysTestRecord;
import com.linkedin.avro.fastserde.primitive.PrimitiveBooleanArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveDoubleArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveIntArrayList;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;

public class FastSerdePrimitivesArrayTest {

    private final Schema schema = PrimitiveArraysTestRecord.SCHEMA$;
    private final FastSpecificDatumReader<PrimitiveArraysTestRecord> reader = new FastSpecificDatumReader<>(
            schema, schema, (SpecificData) null);

    @Test
    void shouldDeserializeUsingPrimitiveLists() throws IOException {
        PrimitiveArraysTestRecord record = new PrimitiveArraysTestRecord();
        FastSerdeTestsSupport.setField(record, "arrayOfInts", Lists.newArrayList(11, 12, 13));
        FastSerdeTestsSupport.setField(record, "arrayOfLongs", Lists.newArrayList(21L, 22L, 23L));
        FastSerdeTestsSupport.setField(record, "arrayOfFloats", Lists.newArrayList(0.1F, 0.2F, 0.3F));
        FastSerdeTestsSupport.setField(record, "arrayOfDoubles", Lists.newArrayList(1.1, 1.1, 1.3));
        FastSerdeTestsSupport.setField(record, "arrayOfBooleans", Lists.newArrayList(true, false, true));

        PrimitiveArraysTestRecord deserializedRecord1 = serializeAndDeserialize(record);

        Assert.assertTrue(getField(deserializedRecord1, "arrayOfInts") instanceof ColdPrimitiveIntList);
        Assert.assertTrue(getField(deserializedRecord1, "arrayOfLongs") instanceof ColdPrimitiveLongList);
        Assert.assertTrue(getField(deserializedRecord1, "arrayOfFloats") instanceof ColdPrimitiveFloatList);
        Assert.assertTrue(getField(deserializedRecord1, "arrayOfDoubles") instanceof ColdPrimitiveDoubleList);
        Assert.assertTrue(getField(deserializedRecord1, "arrayOfBooleans") instanceof ColdPrimitiveBooleanList);

        Awaitility.await()
                .atMost(Durations.TEN_SECONDS)
                .pollInterval(Durations.ONE_HUNDRED_MILLISECONDS)
                .until(() -> {
                    PrimitiveArraysTestRecord deserializedRecord2 = serializeAndDeserialize(record);
                    return getField(deserializedRecord2, "arrayOfInts") instanceof PrimitiveIntArrayList
                            && getField(deserializedRecord2, "arrayOfLongs") instanceof PrimitiveLongArrayList
                            && getField(deserializedRecord2, "arrayOfFloats") instanceof PrimitiveFloatList
                            && getField(deserializedRecord2, "arrayOfDoubles") instanceof PrimitiveDoubleArrayList
                            && getField(deserializedRecord2, "arrayOfBooleans") instanceof PrimitiveBooleanArrayList;
                });
    }

    private PrimitiveArraysTestRecord serializeAndDeserialize(PrimitiveArraysTestRecord record) throws IOException {
        Decoder decoder = FastSerdeTestsSupport.specificDataAsDecoder(record);
        return reader.read(null, decoder);
    }
}
