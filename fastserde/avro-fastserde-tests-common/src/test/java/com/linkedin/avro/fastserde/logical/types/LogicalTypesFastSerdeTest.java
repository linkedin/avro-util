package com.linkedin.avro.fastserde.logical.types;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import com.linkedin.avro.fastserde.Utils;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults;
import com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecord;
import com.linkedin.avro.fastserde.generated.avro.LocalTimestampRecordWithDefaults;
import com.linkedin.avroutil1.compatibility.AvroVersion;

@Ignore
public class LogicalTypesFastSerdeTest extends LogicalTypesTestBase {

    @DataProvider
    public static Object[][] logicalTypesTestCases() {
        LocalDate now = LocalDate.now();
        LocalDate localDate = LocalDate.of(2023, 8, 11);

        Map<CharSequence, LocalDate> mapOfDates = mapOf(
                new String[]{"today", "yesterday", "tomorrow"},
                new LocalDate[]{now, now.minusDays(1), now.plusDays(1)});

        Map<CharSequence, Instant> mapOfTimestamps = mapOf(
                new String[]{"today", "yesterday", "tomorrow"},
                new Instant[]{toInstant(now), toInstant(now.minusDays(1)), toInstant(now.plusDays(1))});

        Map<CharSequence, Object> mapOfDatesAndTimestamps = mapOf(
                new String[]{"today", "yesterday", "tomorrow"},
                new Object[]{toInstant(now), now.minusDays(1), now.plusDays(1)});

        Object[] mapOfUnionsOfDateAndTimestampMillisOptions = {mapOfDates, mapOfTimestamps, mapOfDatesAndTimestamps};
        Object[] nullableUnionOfDateAndLocalTimestampOptions = {null, now.minusDays(12), localDate.atStartOfDay()};
        Object[] unionOfDateAndLocalTimestampOptions = {now.minusDays(12), localDate.atStartOfDay()};
        Object[] unionOfArrayAndMapOptions = {
                Lists.newArrayList(LocalTime.now(), LocalTime.now().plusMinutes(1)), mapOfDates};
        Object[] nullableArrayOfDatesOptions = {
                null, Lists.newArrayList(localDate, localDate.plusDays(11), localDate.plusDays(22))};
        Object[] decimalOrDateOptions = {new BigDecimal("3.14"), LocalDate.of(2023, 3, 14)};

        List<Object[]> allOptions = new ArrayList<>();

        for (Object mapOfUnionsOfDateAndTimestampMillis : mapOfUnionsOfDateAndTimestampMillisOptions) {
            for (Object nullableUnionOfDateAndLocalTimestamp : nullableUnionOfDateAndLocalTimestampOptions) {
                for (Object unionOfDateAndLocalTimestamp : unionOfDateAndLocalTimestampOptions) {
                    for (Object unionOfArrayAndMap : unionOfArrayAndMapOptions) {
                        for (Object nullableArrayOfDates : nullableArrayOfDatesOptions) {
                            for (Object decimalOrDate : decimalOrDateOptions) {
                                allOptions.add(new Object[]{mapOfUnionsOfDateAndTimestampMillis,
                                        nullableUnionOfDateAndLocalTimestamp, unionOfDateAndLocalTimestamp,
                                        unionOfArrayAndMap, nullableArrayOfDates, decimalOrDate});
                            }
                        }
                    }
                }
            }
        }

        return allOptions.toArray(new Object[0][]);
    }

    @Test(groups = "serializationTest", dataProvider = "logicalTypesTestCases")
    public void shouldWriteAndReadLogicalTypesSuccessfully(Map<CharSequence, Object> mapOfUnionsOfDateAndTimestampMillis,
            Object nullableUnionOfDateAndLocalTimestamp, Object unionOfDateAndLocalTimestamp,
            Object unionOfArrayAndMap, List<LocalDate> nullableArrayOfDates, Object decimalOrDate) throws IOException {
        // given
        LocalDate localDate = LocalDate.of(2023, 8, 11);
        Instant instant = localDate.atStartOfDay().toInstant(ZoneOffset.UTC);
        LocalTimestampRecord localTimestampRecord = createLocalTimestampRecord(nullableUnionOfDateAndLocalTimestamp, unionOfDateAndLocalTimestamp);

        FastSerdeLogicalTypesTest1.Builder builder = FastSerdeLogicalTypesTest1.newBuilder()
                .setMapOfUnionsOfDateAndTimestampMillis(mapOfUnionsOfDateAndTimestampMillis)
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
                .setNestedLocalTimestampMillis(localTimestampRecord);
        injectUuidField(builder);
        FastSerdeLogicalTypesTest1 inputData = builder.build();

        // all serializers produce the same array of bytes
        byte[] bytesWithHeader = verifySerializers(inputData, FastSerdeLogicalTypesTest1::toByteBuffer);

        // all deserializers create (logically) the same data (in generic or specific representation)
        verifyDeserializers(bytesWithHeader, FastSerdeLogicalTypesTest1::fromByteBuffer);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "serializationTest")
    public void shouldCreateLogicalTypesFromDefaults() {
        // given
        LocalTime localTime0 = LocalTime.ofNanoOfDay(0L);
        LocalDate localDate0 = LocalDate.ofYearDay(1970, 1);
        LocalDateTime localDateTime0 = LocalDateTime.of(localDate0, localTime0);

        fixConversionsIfAvro19(new FastSerdeLogicalTypesWithDefaults().getSpecificData());

        // when
        FastSerdeLogicalTypesWithDefaults data = FastSerdeLogicalTypesWithDefaults.newBuilder()
                .setNestedLocalTimestampMillisBuilder(LocalTimestampRecordWithDefaults.newBuilder())
                .build();

        // and
        List<LocalTime> arrayOfLocalTimes = (List<LocalTime>) data.getUnionOfArrayAndMap();
        Map<CharSequence, Object> mapOfLocalDates = data.getMapOfUnionsOfDateAndTimestampMillis();
        Map<CharSequence, Instant> timestampMillisMap = data.getTimestampMillisMap();
        List<LocalDate> arrayOfDates = data.getArrayOfDates();
        BigDecimal decimal = (BigDecimal) data.getUnionOfDecimalOrDate();
        Object uuidField = data.getUuidField();
        Instant timestampMillisField = data.getTimestampMillisField();
        Instant timestampMicrosField = data.getTimestampMicrosField();
        LocalTime timeMillisField = data.getTimeMillisField();
        LocalTime timeMicrosField = data.getTimeMicrosField();
        LocalDate dateField = data.getDateField();
        Object nestedTimestamp = data.getNestedLocalTimestampMillis().getNestedTimestamp();
        LocalDate nestedDate = (LocalDate) data.getNestedLocalTimestampMillis().getUnionOfDateAndLocalTimestamp();

        // then
        Assert.assertEquals(arrayOfLocalTimes, Lists.newArrayList(
                localTime0.plus(654321, ChronoUnit.MILLIS),
                localTime0.plus(7415896, ChronoUnit.MILLIS)));

        Assert.assertEquals(mapOfLocalDates, mapOf(
                new Utf8[] {new Utf8("someDay"), new Utf8("anotherDay")},
                new LocalDate[] {localDate0.plusDays(12345), localDate0.plusDays(23456)}));

        Assert.assertEquals(timestampMillisMap, mapOf(
                new Utf8[] {new Utf8("timestampMillis1"), new Utf8("timestampMillis2")},
                new Instant[] {Instant.ofEpochMilli(123456789012L), Instant.ofEpochMilli(112233445566L)}));

        Assert.assertNull(data.getNullableArrayOfDates());
        Assert.assertEquals(arrayOfDates, Lists.newArrayList(localDate0.plusDays(7777), localDate0.plusDays(8888)));
        Assert.assertEquals(decimal.scale(), 2);
        Assert.assertEquals(decimal.precision(), 5);
        Assert.assertEquals(decimal, new BigDecimal(new BigInteger("13".getBytes(StandardCharsets.UTF_8)), 2));
        Assert.assertEquals(uuidField.toString(), "b4ddd079-a024-4cc3-ac6c-a14f174c9922");
        Assert.assertEquals(timestampMillisField, Instant.ofEpochMilli(120120120120L));
        Assert.assertEquals(timestampMicrosField, Instant.ofEpochMilli(0L).plusNanos(123451234512345L * 1000L));
        Assert.assertEquals(timeMillisField, localTime0.plus(15L, ChronoUnit.MILLIS));
        Assert.assertEquals(timeMicrosField, localTime0.plus(16L, ChronoUnit.MICROS));
        Assert.assertEquals(dateField, localDate0.plusDays(223344));
        if (Utils.getRuntimeAvroVersion() == AvroVersion.AVRO_1_9) {
            Assert.assertEquals(nestedTimestamp, 99L);
        } else {
            Assert.assertEquals(nestedTimestamp, localDateTime0.plus(99, ChronoUnit.MILLIS));
        }
        Assert.assertEquals(nestedDate, localDate0.plusDays(45678));
    }

    private Map<CharSequence, Instant> createTimestampMillisMap() {
        return mapOf(new String[]{"one", "two", "three"}, new Instant[]{
                toInstant(LocalDate.of(2023, 8, 18)),
                toInstant(LocalDate.of(2023, 8, 19)),
                toInstant(LocalDate.of(2023, 8, 20))});
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

    private static <K extends CharSequence, V> Map<K, V> mapOf(K[] keys, V[] values) {
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < keys.length; i++) {
            map.put(keys[i], values[i]);
        }

        return map;
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
