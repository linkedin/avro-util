package com.linkedin.avro.fastserde.logical.types;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined;

@Ignore
public class UndefinedLogicalTypesFastSerdeTest extends LogicalTypesTestBase {

    private final LocalTime localTime = LocalTime.now();
    private final LocalDate localDate = LocalDate.now();

    @Test(groups = "serializationTest")
    void definedAndUndefinedLogicalTypesShouldSerializeToTheSameBytes() throws IOException {
        // given
        FastSerdeLogicalTypesDefined logicalTypesDefined = createFastSerdeLogicalTypesDefined();
        FastSerdeLogicalTypesUndefined logicalTypesUndefined = createFastSerdeLogicalTypesUndefined();

        // when
        byte[] bytesFromDefinedLogicalTypes = dropV1Header(verifySerializers(
                logicalTypesDefined, FastSerdeLogicalTypesDefined::toByteBuffer));

        // and
        byte[] bytesFromUndefinedLogicalTypes = dropV1Header(verifySerializers(
                logicalTypesUndefined, FastSerdeLogicalTypesUndefined::toByteBuffer));

        // then
        Assert.assertEquals(bytesFromDefinedLogicalTypes, bytesFromUndefinedLogicalTypes);
    }

    @Test(groups = "deserializationTest")
    void deserializationToDefinedAndUndefinedLogicalTypesShouldBeEquivalent() throws IOException {
        // given
        byte[] bytesWithHeaderAndLogicalTypesDefined = verifySerializers(createFastSerdeLogicalTypesDefined(),
                FastSerdeLogicalTypesDefined::toByteBuffer);
        byte[] bytesWithHeaderAndLogicalTypesUndefined = verifySerializers(createFastSerdeLogicalTypesUndefined(),
                FastSerdeLogicalTypesUndefined::toByteBuffer);

        // assume
        Assert.assertEquals(dropV1Header(bytesWithHeaderAndLogicalTypesDefined), dropV1Header(bytesWithHeaderAndLogicalTypesUndefined));

        // when
        FastSerdeLogicalTypesDefined logicalTypesDefined = verifyDeserializers(bytesWithHeaderAndLogicalTypesDefined,
                FastSerdeLogicalTypesDefined::fromByteBuffer);

        // and
        FastSerdeLogicalTypesUndefined logicalTypesUndefined = verifyDeserializers(bytesWithHeaderAndLogicalTypesUndefined,
                FastSerdeLogicalTypesUndefined::fromByteBuffer);

        // then
        Assert.assertEquals(toInt(logicalTypesDefined.getTimeMillisField()), logicalTypesUndefined.getTimeMillisField());
        Assert.assertEquals(toInt(logicalTypesDefined.getDateField()), logicalTypesUndefined.getDateField());
        assertArraysOfUnionsAreEquivalent(logicalTypesDefined.getArrayOfUnionOfDateAndTimestampMillis(),
                logicalTypesUndefined.getArrayOfUnionOfDateAndTimestampMillis());
    }

    private void assertArraysOfUnionsAreEquivalent(List<Object> unionsOfLogicalTypes, List<Object> unionsOfPrimitiveTypes) {
        Assert.assertEquals(unionsOfLogicalTypes.size(), unionsOfPrimitiveTypes.size());

        for (int i = 0; i < unionsOfLogicalTypes.size(); i++) {
            Object logical = unionsOfLogicalTypes.get(i);
            Object primitive = unionsOfPrimitiveTypes.get(i);

            if (logical instanceof LocalDate) {
                Assert.assertEquals(toInt((LocalDate) logical), ((Integer) primitive).intValue());
            } else if (logical instanceof Instant) {
                Assert.assertEquals(((Instant) logical).toEpochMilli(), ((Long) primitive).longValue());
            } else {
                throw new RuntimeException("Unknown class of logical-type value: " + logical.getClass());
            }
        }
    }

    private FastSerdeLogicalTypesDefined createFastSerdeLogicalTypesDefined() {
        return FastSerdeLogicalTypesDefined.newBuilder()
                .setTimeMillisField(localTime)
                .setDateField(localDate)
                .setArrayOfUnionOfDateAndTimestampMillis(Lists.newArrayList(
                        LocalDate.of(2023, 8, 21),
                        LocalDate.of(2023, 8, 22),
                        toInstant(localDate),
                        toInstant(localDate.plusDays(7)),
                        LocalDate.of(2023, 12, 31)))
                .build();
    }

    // same as above but without logical types
    private FastSerdeLogicalTypesUndefined createFastSerdeLogicalTypesUndefined() {
        return FastSerdeLogicalTypesUndefined.newBuilder()
                .setTimeMillisField(toInt(localTime))
                .setDateField(toInt(localDate))
                .setArrayOfUnionOfDateAndTimestampMillis(Lists.newArrayList(
                        toInt(LocalDate.of(2023, 8, 21)),
                        toInt(LocalDate.of(2023, 8, 22)),
                        toInstant(localDate).toEpochMilli(),
                        toInstant(localDate.plusDays(7)).toEpochMilli(),
                        toInt(LocalDate.of(2023, 12, 31))))
                .build();
    }

    private int toInt(LocalTime localTime) {
        return (int) TimeUnit.NANOSECONDS.toMillis(localTime.toNanoOfDay());
    }

    private int toInt(LocalDate localDate) {
        return (int) localDate.toEpochDay();
    }
}
