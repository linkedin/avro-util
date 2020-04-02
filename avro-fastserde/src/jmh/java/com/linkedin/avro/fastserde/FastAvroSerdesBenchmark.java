package com.linkedin.avro.fastserde;

import com.linkedin.avro.fastserde.generated.avro.BenchmarkSchema;
import com.linkedin.avro.fastserde.generator.AvroRandomDataGenerator;
import com.linkedin.avro.fastserde.micro.benchmark.AvroGenericDeserializer;
import com.linkedin.avro.fastserde.micro.benchmark.AvroGenericSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;


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
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class FastAvroSerdesBenchmark {
  private final Random random = new Random();;
  private final Map<Object, Object> properties = new HashMap<>();

  private byte[] serializedBytes;
  private GenericData.Record generatedRecord;

  private final Schema benchmarkSchame = BenchmarkSchema.SCHEMA$;
  private static AvroRandomDataGenerator generator;

  private static AvroGenericSerializer serializer;
  private static AvroGenericSerializer fastSerializer;
  private static AvroGenericDeserializer<GenericRecord> deserializer;
  private static AvroGenericDeserializer<GenericRecord> fastDeserializer;

  public FastAvroSerdesBenchmark() {
    // load configuration parameters to avro data generator
    properties.put(AvroRandomDataGenerator.ARRAY_LENGTH_PROP, BenchmarkConstants.ARRAY_SIZE);
    properties.put(AvroRandomDataGenerator.STRING_LENGTH_PROP, BenchmarkConstants.STRING_SIZE);
    properties.put(AvroRandomDataGenerator.BYTES_LENGTH_PROP, BenchmarkConstants.BYTES_SIZE);
    properties.put(AvroRandomDataGenerator.MAP_LENGTH_PROP, BenchmarkConstants.MAP_SIZE);
    generator = new AvroRandomDataGenerator(benchmarkSchame, random);
  }

  public byte[] serializeGeneratedRecord(GenericData.Record generatedRecord) throws Exception {
    AvroGenericSerializer serializer = new AvroGenericSerializer(benchmarkSchame);
    return serializer.serialize(generatedRecord);
  }

  @Setup(Level.Trial)
  public void prepare() throws Exception {
    // generate avro record and bytes data
    generatedRecord = (GenericData.Record) generator.generate(properties);
    serializedBytes = serializeGeneratedRecord(generatedRecord);

    serializer = new AvroGenericSerializer(benchmarkSchame);
    fastSerializer = new AvroGenericSerializer(new FastGenericDatumWriter<>(benchmarkSchame));
    deserializer = new AvroGenericDeserializer<>(new GenericDatumReader<>(benchmarkSchame));
    fastDeserializer = new AvroGenericDeserializer<>(new FastGenericDatumReader<>(benchmarkSchame));
  }

  @Benchmark
  public void testAvroSerialization() throws Exception {
    // use vanilla avro 1.4 encoder, do not use buffer binary encoder
    serializer.serialize(generatedRecord);
  }

  @Benchmark
  public void testFastAvroSerialization() throws Exception {
    fastSerializer.serialize(generatedRecord);
  }

  @Benchmark
  public void testAvroDeserialization() throws Exception {
    deserializer.deserialize(serializedBytes);
  }

  @Benchmark
  public void testFastAvroDeserialization() throws Exception {
    fastDeserializer.deserialize(serializedBytes);
  }
}
