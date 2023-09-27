package com.linkedin.avro.fastserde.micro.benchmark;

import com.linkedin.avro.fastserde.FastGenericDatumReader;
import com.linkedin.avro.fastserde.FastGenericDatumWriter;
import com.linkedin.avro.fastserde.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SerDeMicroBenchmark {

  static {
    System.out.println("Current avro version: " + Utils.getRuntimeAvroVersion());
  }

  public static BenchmarkTestObject newTestObject() {
    BenchmarkTestObject newObj = new BenchmarkTestObject();
    List<Schema.Field> fields = newObj.getSchema().getFields();
    List<Float> arrayFloatDefault = new ArrayList<>();

    for (int i = 0; i < 20; ++i) {
      arrayFloatDefault.add(0.422355f);
    }

    fields.forEach(field -> {
      Schema.Type type = field.schema().getType();
      if (type.equals(Schema.Type.FLOAT)) {
        newObj.put(field.pos(), 100f);
      } else if (type.equals(Schema.Type.INT)) {
        newObj.put(field.pos(), 100);
      } else if (type.equals(Schema.Type.STRING)) {
        newObj.put(field.pos(), "testsetesttestsetestsetesttestsette");
      } else {
        // array of float
        newObj.put(field.pos(), arrayFloatDefault);
      }
    });

    return newObj;
  }

  public static byte[] serializedTestObjects(int recordCnt) throws Exception {
    List<BenchmarkTestObject> objs = new ArrayList<>();
    for (int i = 0; i < recordCnt; ++i) {
      objs.add(newTestObject());
    }
    AvroGenericSerializer serializer = new AvroGenericSerializer(BenchmarkTestObject.SCHEMA$);
    return serializer.serializeObjects(objs);
  }

  @Test(invocationCount = 5, groups = {"perfTest"})
  public void testAvroSerialization() throws Exception {
    long startInMs = System.currentTimeMillis();

    int recordCnt = 1000;
    List<BenchmarkTestObject> objs = new ArrayList<>();
    for (int j = 0; j < recordCnt; ++j) {
      objs.add(newTestObject());
    }
    AvroGenericSerializer serializer = new AvroGenericSerializer(BenchmarkTestObject.SCHEMA$);
    for (int i = 0; i < 10000; ++i) {
      serializer.serializeObjects(objs);
    }
    System.out.println("Regular avro serialization latency: " + (System.currentTimeMillis() - startInMs) + " ms");
  }

  @Test(invocationCount = 5, groups = {"perfTest"})
  public void testFastAvroSerialization() throws Exception {
    long startInMs = System.currentTimeMillis();

    int recordCnt = 1000;
    List<BenchmarkTestObject> objs = new ArrayList<>();
    for (int j = 0; j < recordCnt; ++j) {
      objs.add(newTestObject());
    }
    AvroGenericSerializer serializer =
        new AvroGenericSerializer(new FastGenericDatumWriter<>(BenchmarkTestObject.SCHEMA$));
    for (int i = 0; i < 10000; ++i) {
      serializer.serializeObjects(objs);
    }
    System.out.println("Fast avro serialization latency: " + (System.currentTimeMillis() - startInMs) + " ms");
  }

  @Test(invocationCount = 5, groups = {"perfTest"})
  public void testAvroDeserialization() throws Exception {
    byte[] serializedBytes = serializedTestObjects(1000);
    long startInMs = System.currentTimeMillis();
    AvroGenericDeserializer<BenchmarkTestObject> deserializer =
        new AvroGenericDeserializer<>(new SpecificDatumReader<>(BenchmarkTestObject.class));
    for (int i = 0; i <= 10000; ++i) {
      deserializer.deserializeObjects(serializedBytes);
    }
    System.out.println("Regular avro deserialization latency: " + (System.currentTimeMillis() - startInMs) + " ms");
  }

  @Test(invocationCount = 5, groups = {"perfTest"})
  public void testFastAvroDeserialization() throws Exception {
    byte[] serializedBytes = serializedTestObjects(1000);
    long startInMs = System.currentTimeMillis();
//    FastSpecificDatumReader<BenchmarkTestObject> fastSpecificDatumReader = new FastSpecificDatumReader<>(BenchmarkTestObject.SCHEMA$);
    FastGenericDatumReader<GenericRecord> fastGenericDatumReader =
        new FastGenericDatumReader<>(BenchmarkTestObject.SCHEMA$);
    AvroGenericDeserializer<GenericRecord> fastDeserializer = new AvroGenericDeserializer<>(fastGenericDatumReader);
    for (int i = 0; i <= 10000; ++i) {
      fastDeserializer.deserializeObjects(serializedBytes);
    }
    System.out.println("Fast avro deserialization latency: " + (System.currentTimeMillis() - startInMs) + " ms");
  }

  @DataProvider(name = "useFastAvroOptionsProvider")
  public Object[][] useFastAvroOptions() {
    // Run {true}, {false} together will produce biased results since the cpu resource might not be
    // recovered yet after the first run.
//    return new Object[][]{{true}};
    return new Object[][]{{false}};
  }

  @Test(dataProvider = "useFastAvroOptionsProvider", groups = {"perfTest"})
  public void testFastAvroWithMultiThread(boolean useFastAvro) throws Exception {
    byte[] serializedBytes = serializedTestObjects(1000);
    AvroGenericDeserializer<BenchmarkTestObject> deserializer;
    if (useFastAvro) {
      FastGenericDatumReader<BenchmarkTestObject> fastGenericDatumReader =
          new FastGenericDatumReader<>(BenchmarkTestObject.SCHEMA$);
      deserializer = new AvroGenericDeserializer<>(fastGenericDatumReader);
    } else {
      deserializer = new AvroGenericDeserializer<>(new GenericDatumReader<>(BenchmarkTestObject.SCHEMA$));
    }

    long startTs = System.currentTimeMillis();

    int threadCnt = 20;
    ExecutorService executorService = Executors.newFixedThreadPool(threadCnt);
    AtomicInteger totalFetchCnt = new AtomicInteger();
    for (int i = 0; i < threadCnt; ++i) {
      final AvroGenericDeserializer<BenchmarkTestObject> finalDeserializer = deserializer;
      executorService.submit(() -> {
        while (true) {
          try {
            finalDeserializer.deserializeObjects(serializedBytes);
            totalFetchCnt.incrementAndGet();
          } catch (InterruptedException e) {
            break;
          } catch (ExecutionException e) {
            e.printStackTrace();
            break;
          } catch (Exception e) {
            e.printStackTrace();
            break;
          }
        }
      });
    }

    executorService.awaitTermination(30, TimeUnit.SECONDS);
    System.out.println("This test with useFastAvro: " + useFastAvro + " took " + (System.currentTimeMillis() - startTs)
        + "ms, and it finished " + totalFetchCnt.get() + " requests");
  }
}
