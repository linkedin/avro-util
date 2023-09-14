package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.FloatArrayBenchmarkSchema;
import com.linkedin.avro.fastserde.generator.AvroRandomDataGenerator;
import com.linkedin.avro.fastserde.micro.benchmark.AvroGenericSerializer;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.List;
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
 * A benchmark that evaluates the performance of PrimitiveFloatList access
 *
 * To run this benchmark:
 * <code>
 *   ./gradlew :fastserde:avro-fastserde-jmh:jmh -PUSE_AVRO_18
 * </code>
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
//@Fork(value = 1, jvmArgsAppend = {"-XX:+PrintGCDetails", "-Xms16g", "-Xmx16g"})
//@Threads(10)
@Warmup(iterations = 3)
@Measurement(iterations = 3)
public class FloatArrayBenchmark {
  private static final int NUMBER_OF_OPERATIONS = 100_000;

  private final Random random = new Random();;
  private final Map<Object, Object> properties = new HashMap<>();

  private byte[] serializedBytes;
  private GenericData.Record generatedRecord;

  private final Schema benchmarkSchema = FloatArrayBenchmarkSchema.SCHEMA$;
  private static AvroRandomDataGenerator generator;

  private static DatumReader<GenericRecord> fastDeserializer;

  public FloatArrayBenchmark() {
    // load configuration parameters to avro data generator
    properties.put(AvroRandomDataGenerator.ARRAY_LENGTH_PROP, BenchmarkConstants.FLOAT_ARRAY_SIZE);
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

    fastDeserializer = new FastGenericDatumReader<>(benchmarkSchema);
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
  public void testFastAvroDeserializationAccess1(Blackhole bh) throws Exception {
    testFastFloatArrayDeserialization(fastDeserializer, bh, 1);
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
  public void testFastAvroDeserializationAccess8(Blackhole bh) throws Exception {
    testFastFloatArrayDeserialization(fastDeserializer, bh, 8);
  }

  @Benchmark
  @OperationsPerInvocation(NUMBER_OF_OPERATIONS)
  public void testFastAvroDeserializationAccess16(Blackhole bh) throws Exception {
    testFastFloatArrayDeserialization(fastDeserializer, bh, 16);
  }

  private void testFastFloatArrayDeserialization(DatumReader<GenericRecord> datumReader, Blackhole bh, int numElementAccess) throws Exception {
    GenericRecord record = null;
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(serializedBytes);
    for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
      float w = 0;
      decoder = AvroCompatibilityHelper.newBinaryDecoder(new ByteArrayInputStream(serializedBytes), false, decoder);
      record = datumReader.read(record, decoder);
      if (numElementAccess > 0) {
        List<Float> list = (List<Float>)record.get(0);
        for (int j = 0; j < list.size(); j += numElementAccess) {
          w += list.get(j);
        }
      }
      bh.consume(record);
      bh.consume(w);
    }
  }
}
