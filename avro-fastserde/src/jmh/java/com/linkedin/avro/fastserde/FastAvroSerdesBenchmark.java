package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.BenchmarkSchema;
import com.linkedin.avro.fastserde.generator.AvroRandomDataGenerator;
import com.linkedin.avro.fastserde.micro.benchmark.AvroGenericSerializer;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * A benchmark that evaluates the performance of serialize and deserialize of the given Avro Schema and
 * parameters using original Apache Avro GenericDatumReader/Writer and FastGenericDatumReader/Writer in
 * different Avro versions
 *
 * To run this benchmark:
 * <code>
 *   ./gradlew :avro-fastserde:jmh -PUSE_AVRO_14/17/18
 * </code>
 *
 * You also can test by your own AVRO schema by replacing the contents in
 *   avro-util/avro-fastserde/src/test/avro/benchmarkSchema.avsc
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
//@Fork(value = 1, jvmArgsAppend = {"-XX:+PrintGCDetails", "-Xms16g", "-Xmx16g"})
//@Threads(10)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
public class FastAvroSerdesBenchmark {
  private static final int NUMBER_OF_OPERATIONS = 100_000;

  private final Random random = new Random();;
  private final Map<Object, Object> properties = new HashMap<>();

  private byte[] serializedBytes;
  private GenericData.Record generatedRecord;

  private final Schema benchmarkSchema = BenchmarkSchema.SCHEMA$;
  private static AvroRandomDataGenerator generator;

  private static AvroGenericSerializer serializer;
  private static AvroGenericSerializer fastSerializer;
  private static DatumReader<GenericRecord> deserializer;
  private static DatumReader<GenericRecord> fastDeserializer;

  public FastAvroSerdesBenchmark() {
    // load configuration parameters to avro data generator
    properties.put(AvroRandomDataGenerator.ARRAY_LENGTH_PROP, BenchmarkConstants.ARRAY_SIZE);
    properties.put(AvroRandomDataGenerator.STRING_LENGTH_PROP, BenchmarkConstants.STRING_SIZE);
    properties.put(AvroRandomDataGenerator.BYTES_LENGTH_PROP, BenchmarkConstants.BYTES_SIZE);
    properties.put(AvroRandomDataGenerator.MAP_LENGTH_PROP, BenchmarkConstants.MAP_SIZE);
    generator = new AvroRandomDataGenerator(benchmarkSchema, random);
  }

  public static void main(String[] args) throws RunnerException {
    org.openjdk.jmh.runner.options.Options opt = new OptionsBuilder()
        .include(FastAvroSerdesBenchmark.class.getSimpleName())
        .addProfiler(GCProfiler.class)
        .build();
    new Runner(opt).run();
  }

  public byte[] serializeGeneratedRecord(GenericData.Record generatedRecord) throws Exception {
    AvroGenericSerializer serializer = new AvroGenericSerializer(benchmarkSchema);
    return serializer.serialize(generatedRecord);
  }

  @Setup(Level.Trial)
  public void prepare() throws Exception {
    // generate avro record and bytes data
    generatedRecord = (GenericData.Record) generator.generate(properties);
    serializedBytes = serializeGeneratedRecord(generatedRecord);

    serializer = new AvroGenericSerializer(benchmarkSchema);
    fastSerializer = new AvroGenericSerializer(new FastGenericDatumWriter<>(benchmarkSchema));
    deserializer = new GenericDatumReader<>(benchmarkSchema);
    fastDeserializer = new FastGenericDatumReader<>(benchmarkSchema);
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
  public void testAvroSerialization(Blackhole bh) throws Exception {
    for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
      // use vanilla avro 1.4 encoder, do not use buffer binary encoder
      bh.consume(serializer.serialize(generatedRecord));
    }
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
  public void testFastAvroSerialization(Blackhole bh) throws Exception {
    for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
      bh.consume(fastSerializer.serialize(generatedRecord));
    }
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
  public void testAvroDeserialization(Blackhole bh) throws Exception {
    testDeserialization(deserializer, bh);
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
  public void testFastAvroDeserialization(Blackhole bh) throws Exception {
    testDeserialization(fastDeserializer, bh);
  }

  private void testDeserialization(DatumReader<GenericRecord> datumReader, Blackhole bh) throws Exception {
    GenericRecord record = null;
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(serializedBytes);
    for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
      decoder = AvroCompatibilityHelper.newBinaryDecoder(new ByteArrayInputStream(serializedBytes), false, decoder);
      record = datumReader.read(record, decoder);
      bh.consume(record);
    }
  }
}
