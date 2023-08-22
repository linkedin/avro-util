package com.linkedin.avro.fastserde.logical.types;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.testng.Assert;
import org.testng.collections.Lists;

import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.avro.fastserde.FastSerializer;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.SchemaNormalization;

public abstract class LogicalTypesTestBase {

    protected final int v1HeaderLength = 10;

    protected <T extends GenericData> T copyConversions(SpecificData fromSpecificData, T toModelData) {
        Optional.ofNullable(fromSpecificData.getConversions())
                .orElse(Collections.emptyList())
                .forEach(toModelData::addLogicalTypeConversion);

        fixConversionsIfAvro19(toModelData);

        return toModelData;
    }

    protected <T extends GenericData> void fixConversionsIfAvro19(T modelData) {
        if (AvroCompatibilityHelperCommon.getRuntimeAvroVersion() == AvroVersion.AVRO_1_9) {
            // TODO these conversions should be injected by default.
            // Missing DecimalConversion causes conflict with stringable feature (BigDecimal is considered as string, not bytes)
            // details: GenericData.resolveUnion() + SpecificData.getSchemaName()

            List<Conversion<?>> requiredConversions = Lists.newArrayList(
                    new Conversions.DecimalConversion(),
                    new TimeConversions.TimeMillisConversion(),
                    new TimeConversions.TimeMicrosConversion(),
                    new TimeConversions.TimestampMicrosConversion());

            // LocalTimestampMillisConversion not available in 1.9

            for (Conversion<?> conversion : requiredConversions) {
                modelData.addLogicalTypeConversion(conversion);
            }
        }

        // TODO in 1.10 field of type UUID is generated as CharSequence
        // toModelData.addLogicalTypeConversion(new Conversions.UUIDConversion());
    }

    protected <T> byte[] serialize(FastSerializer<T> fastSerializer, T data) throws IOException {
        InMemoryEncoder encoder = new InMemoryEncoder();
        fastSerializer.serialize(data, encoder);
        return encoder.toByteArray();
    }

    protected <T> byte[] serialize(DatumWriter<T> datumWriter, T data) throws IOException {
        InMemoryEncoder encoder = new InMemoryEncoder();
        datumWriter.write(data, encoder);

        return encoder.toByteArray();
    }

    protected Instant toInstant(Object maybeDate) {
        if (maybeDate == null) {
            return null;
        } else if (maybeDate instanceof LocalDate) {
            return ((LocalDate) maybeDate).atStartOfDay(ZoneId.systemDefault()).toInstant();
        } else if (maybeDate instanceof LocalDateTime) {
            return ((LocalDateTime) maybeDate).toInstant(ZoneOffset.UTC);
        } else {
            throw new UnsupportedOperationException(maybeDate + " is not supported (yet)");
        }
    }

    /**
     * Runs various serializers and ensures each one returned the same result.
     * @return result from default serialization, normally it's just {@code data.toByteBuffer().array()}
     */
    protected <T extends SpecificRecordBase> byte[] verifySerializers(T data,
            FunctionThrowingIOException<T, ByteBuffer> toByteBuffer) throws IOException {
        // given
        FastSerdeCache fastSerdeCache = FastSerdeCache.getDefaultInstance();

        @SuppressWarnings("unchecked")
        FastSerializer<T> fastGenericSerializer = (FastSerializer<T>) fastSerdeCache
                .buildFastGenericSerializer(data.getSchema(), copyConversions(data.getSpecificData(), new GenericData()));

        @SuppressWarnings("unchecked")
        FastSerializer<T> fastSpecificSerializer = (FastSerializer<T>) fastSerdeCache
                .buildFastSpecificSerializer(data.getSchema(), copyConversions(data.getSpecificData(), new SpecificData()));

        GenericDatumWriter<T> genericDatumWriter = new GenericDatumWriter<>(
                data.getSchema(), copyConversions(data.getSpecificData(), new GenericData()));

        SpecificDatumWriter<T> specificDatumWriter = new SpecificDatumWriter<>(
                data.getSchema(), copyConversions(data.getSpecificData(), new SpecificData()));

        fixConversionsIfAvro19(data.getSpecificData());

        // when
        byte[] fastGenericBytes = serialize(fastGenericSerializer, data);
        byte[] fastSpecificBytes = serialize(fastSpecificSerializer, data);
        byte[] genericBytes = serialize(genericDatumWriter, data);
        byte[] specificBytes = serialize(specificDatumWriter, data);
        byte[] bytesWithHeader = toByteBuffer.apply(data).array(); // contains 10 extra bytes at the beginning

        byte[] expectedHeaderBytes = ByteBuffer.wrap(new byte[v1HeaderLength])
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(new byte[]{(byte) 0xC3, (byte) 0x01}) // BinaryMessageEncoder.V1_HEADER
                .putLong(SchemaNormalization.parsingFingerprint64(data.getSchema()))
                .array();

        // then all 5 serializing methods should return the same array of bytes
        Assert.assertEquals(Arrays.copyOf(bytesWithHeader, v1HeaderLength), expectedHeaderBytes);
        Assert.assertEquals(fastGenericBytes, dropV1Header(bytesWithHeader));
        Assert.assertEquals(fastGenericBytes, fastSpecificBytes);
        Assert.assertEquals(fastGenericBytes, genericBytes);
        Assert.assertEquals(fastGenericBytes, specificBytes);

        return bytesWithHeader;
    }

    protected <T extends SpecificRecordBase> T verifyDeserializers(byte[] bytesWithHeader,
            FunctionThrowingIOException<ByteBuffer, T> fromByteBuffer) throws IOException {
        // given
        T data = fromByteBuffer.apply(ByteBuffer.wrap(bytesWithHeader));
        byte[] bytes = dropV1Header(bytesWithHeader);
        Schema schema = data.getSchema();
        Supplier<Decoder> decoderSupplier = () -> DecoderFactory.get().binaryDecoder(bytes, null);

        FastSerdeCache fastSerdeCache = FastSerdeCache.getDefaultInstance();

        @SuppressWarnings("unchecked")
        FastDeserializer<GenericData.Record> fastGenericDeserializer = (FastDeserializer<GenericData.Record>) fastSerdeCache
                .buildFastGenericDeserializer(schema, schema, copyConversions(data.getSpecificData(), new GenericData()));

        @SuppressWarnings("unchecked")
        FastDeserializer<T> fastSpecificDeserializer = (FastDeserializer<T>) fastSerdeCache
                .buildFastSpecificDeserializer(schema, schema, copyConversions(data.getSpecificData(), new SpecificData()));

        GenericDatumReader<GenericData.Record> genericDatumReader = new GenericDatumReader<>(
                schema, schema, copyConversions(data.getSpecificData(), new GenericData()));

        SpecificDatumReader<T> specificDatumReader = new SpecificDatumReader<>(
                schema, schema, copyConversions(data.getSpecificData(), new SpecificData()));

        // when deserializing with different serializers/writers
        GenericData.Record deserializedFastGeneric = fastGenericDeserializer.deserialize(decoderSupplier.get());
        T deserializedFastSpecific = fastSpecificDeserializer.deserialize(decoderSupplier.get());
        GenericData.Record deserializedGenericReader = genericDatumReader.read(null, decoderSupplier.get());
        T deserializedSpecificReader = specificDatumReader.read(null, decoderSupplier.get());

        // then
        Assert.assertEquals(deserializedFastSpecific, data);
        Assert.assertEquals(deserializedSpecificReader, data);
        assertEquals(deserializedFastGeneric, data);
        assertEquals(deserializedGenericReader, data);

        return deserializedFastSpecific;
    }

    protected <T extends SpecificRecordBase> void assertEquals(GenericData.Record actual, T expected) throws IOException {
        Assert.assertEquals(actual.toString(), expected.toString());

        GenericDatumWriter<GenericData.Record> genericDatumWriter = new GenericDatumWriter<>(
                actual.getSchema(), copyConversions(expected.getSpecificData(), new GenericData()));

        SpecificDatumWriter<T> specificDatumWriter = new SpecificDatumWriter<>(
                expected.getSchema(), copyConversions(expected.getSpecificData(), new SpecificData()));

        byte[] genericBytes = serialize(genericDatumWriter, actual);
        byte[] expectedBytes = serialize(specificDatumWriter, expected);

        Assert.assertEquals(genericBytes, expectedBytes);
    }

    protected byte[] dropV1Header(byte[] bytesWithHeader) {
        return Arrays.copyOfRange(bytesWithHeader, v1HeaderLength, bytesWithHeader.length);
    }
}
