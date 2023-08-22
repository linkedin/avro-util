package com.linkedin.avro.fastserde.logical.types;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.linkedin.avro.fastserde.Utils;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1;
import com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord;
import com.linkedin.avroutil1.compatibility.AvroVersion;

@Ignore
public class LogicalTypesFastSerdeTest extends LogicalTypesTestBase {

    @DataProvider
    public static Object[][] logicalTypesTestCases() {
        LocalDate now = LocalDate.now();
        LocalDate localDate = LocalDate.of(2023, 8, 11);

        Map<String, LocalDate> mapOfDates = new HashMap<>();
        mapOfDates.put("today", now);
        mapOfDates.put("yesterday", now.minusDays(1));
        mapOfDates.put("tomorrow", now.plusDays(1));

        Object[] unionOfArrayAndMapOptions = {
                Lists.newArrayList(LocalTime.now(), LocalTime.now().plusMinutes(1)), mapOfDates};
        Object[] nullableArrayOfDatesOptions = {
                null, Lists.newArrayList(localDate, localDate.plusDays(11), localDate.plusDays(22))};
        Object[] decimalOrDateOptions = {new BigDecimal("3.14"), LocalDate.of(2023, 3, 14)};
        Object[] nullableUnionOfDateAndLocalTimestampOptions = {null, now.minusDays(12), localDate.atStartOfDay()};
        Object[] unionOfDateAndLocalTimestampOptions = {now.minusDays(12), localDate.atStartOfDay()};

        List<Object[]> allOptions = new ArrayList<>();

        for (Object unionOfArrayAndMap : unionOfArrayAndMapOptions) {
            for (Object nullableArrayOfDates : nullableArrayOfDatesOptions) {
                for (Object decimalOrDate : decimalOrDateOptions) {
                    for (Object nullableUnionOfDateAndLocalTimestamp: nullableUnionOfDateAndLocalTimestampOptions) {
                        for (Object unionOfDateAndLocalTimestamp : unionOfDateAndLocalTimestampOptions) {
                            allOptions.add(new Object[]{unionOfArrayAndMap, nullableArrayOfDates, decimalOrDate,
                                    nullableUnionOfDateAndLocalTimestamp, unionOfDateAndLocalTimestamp});
                        }
                    }
                }
            }
        }

        return allOptions.toArray(new Object[0][]);
    }

    @Test(groups = "serializationTest", dataProvider = "logicalTypesTestCases")
    public void shouldWriteAndReadLogicalTypesSuccessfully(Object unionOfArrayAndMap, List<LocalDate> nullableArrayOfDates,
            Object decimalOrDate, Object nullableUnionOfDateAndLocalTimestamp, Object unionOfDateAndLocalTimestamp) throws IOException {
        // given
        LocalDate localDate = LocalDate.of(2023, 8, 11);
        Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);

        FastSerdeLogicalTypesTest1.Builder builder = FastSerdeLogicalTypesTest1.newBuilder()
                .setUnionOfArrayAndMap(unionOfArrayAndMap)
                .setTimestampMillisMap(createTimestampMillisMap())
                .setNullableArrayOfDates(nullableArrayOfDates)
                .setArrayOfDates(Lists.newArrayList(localDate, localDate.plusDays(1), localDate.plusDays(2)))
                .setUnionOfDecimalOrDate(decimalOrDate)
                .setTimestampMillisField(instant)
                .setTimestampMicrosField(instant)
                .setTimeMillisField(LocalTime.of(14, 17, 45, 12345))
                .setTimeMicrosField(LocalTime.of(14, 17, 45, 12345))
                .setDateField(localDate)
                .setNestedLocalTimestampMillis(createLocalTimestampRecord(nullableUnionOfDateAndLocalTimestamp, unionOfDateAndLocalTimestamp));
        injectUuidField(builder);
        FastSerdeLogicalTypesTest1 inputData = builder.build();

        // all serializers produce the same array of bytes
        byte[] bytesWithHeader = verifySerializers(inputData, FastSerdeLogicalTypesTest1::toByteBuffer);

        // all deserializers create (logically) the same data (in generic or specific representation)
        verifyDeserializers(bytesWithHeader, FastSerdeLogicalTypesTest1::fromByteBuffer);
    }

    private Map<CharSequence, Instant> createTimestampMillisMap() {
        Map<CharSequence, Instant> map = new HashMap<>();
        map.put("one", toInstant(LocalDate.of(2023, 8, 18)));
        map.put("two", toInstant(LocalDate.of(2023, 8, 19)));
        map.put("three", toInstant(LocalDate.of(2023, 8, 20)));

        return map;
    }

    private LocalTimestampRecord createLocalTimestampRecord(
            Object nullableUnionOfDateAndLocalTimestamp, Object unionOfDateAndLocalTimestamp) {
        Instant nestedTimestamp = toInstant(LocalDate.of(2023, 8, 21));
        LocalTimestampRecord.Builder builder = LocalTimestampRecord.newBuilder();

        try {
            if (Utils.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_9)) {
                builder.getClass().getMethod("setNestedTimestamp", LocalDateTime.class)
                        .invoke(builder, LocalDateTime.ofInstant(nestedTimestamp, ZoneId.systemDefault()));
                builder.getClass().getMethod("setNullableNestedTimestamp", LocalDateTime.class)
                        .invoke(builder, LocalDateTime.ofInstant(nestedTimestamp.plusSeconds(10), ZoneId.systemDefault()));
            } else {
                nullableUnionOfDateAndLocalTimestamp = Optional.ofNullable(toInstant(nullableUnionOfDateAndLocalTimestamp))
                        .map(Instant::toEpochMilli)
                        .orElse(null);
                unionOfDateAndLocalTimestamp = toInstant(unionOfDateAndLocalTimestamp).toEpochMilli();

                builder.getClass().getMethod("setNestedTimestamp", Long.TYPE)
                        .invoke(builder, nestedTimestamp.toEpochMilli());
                builder.getClass().getMethod("setNullableNestedTimestamp", Long.class)
                        .invoke(builder, nestedTimestamp.toEpochMilli() + 10L);
            }

            builder.setNullableUnionOfDateAndLocalTimestamp(nullableUnionOfDateAndLocalTimestamp);
            builder.setUnionOfDateAndLocalTimestamp(unionOfDateAndLocalTimestamp);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        return builder.build();
    }

    private void injectUuidField(FastSerdeLogicalTypesTest1.Builder builder) {
        try {
            if (Utils.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_10)) {
                builder.getClass().getMethod("setUuidField", UUID.class)
                        .invoke(builder, UUID.randomUUID());
            } else {
                builder.getClass().getMethod("setUuidField", CharSequence.class)
                        .invoke(builder, UUID.randomUUID().toString());
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
