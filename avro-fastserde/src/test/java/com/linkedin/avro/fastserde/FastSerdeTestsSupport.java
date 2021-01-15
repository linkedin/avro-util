package com.linkedin.avro.fastserde;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroSchemaUtil;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;


public final class FastSerdeTestsSupport {

  public static final Schema SCHEMA_FOR_TEST_ENUM =
      Schema.createEnum("TestEnum", "", "com.linkedin.avro.fastserde.generated.avro",
          Arrays.asList("A", "B", "C", "D", "E"));

  private FastSerdeTestsSupport() {
  }

  /**
   * Ths function will infer a standardized name for the generated record, to help make generated deserializers
   * more easily recognizable...
   *
   * @param fields
   * @return
   */
  public static Schema createRecord(Schema.Field... fields) {
    // Function name retrieval magic lifted from: https://stackoverflow.com/a/4065546
    StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
    StackTraceElement e = stacktrace[2]; //maybe this number needs to be corrected
    String methodName = e.getMethodName();

    String className = e.getClassName();
    String simpleClassName = className.substring(className.lastIndexOf("."));

    String recordName = simpleClassName + "_" + methodName;
    return createRecord(recordName, fields);
  }

  public static Schema createRecord(String name, Schema.Field... fields) {
    Schema schema = Schema.createRecord(name, name, "com.adpilot.utils.generated.avro", false);
    schema.setFields(Arrays.asList(fields));

    return schema;
  }

  public static Schema.Field createField(String name, Schema schema) {
    return new Schema.Field(name, schema, "", null, Schema.Field.Order.ASCENDING);
  }

  public static Schema.Field createUnionFieldWithNull(String name, Schema... schemas) {
    Schema[] schemasWithNull = new Schema[schemas.length + 1];
    schemasWithNull[0] = Schema.create(Schema.Type.NULL);
    for (int i = 0; i < schemas.length; i++) {
      schemasWithNull[i + 1] = schemas[i];
    }
    return createUnionField(name, schemasWithNull);
  }

  public static Schema.Field createUnionField(String name, Schema... schemas) {
    List<Schema> typeList = new ArrayList<>();
    typeList.addAll(Arrays.asList(schemas));

    Schema unionSchema = Schema.createUnion(typeList);
    return new Schema.Field(name, unionSchema, null, null, Schema.Field.Order.ASCENDING);
  }

  public static Schema.Field createPrimitiveFieldSchema(String name, Schema.Type type) {
    return new Schema.Field(name, Schema.create(type), null, null);
  }

  public static Schema.Field createPrimitiveUnionFieldSchema(String name, Schema.Type... types) {
    List<Schema> typeList = new ArrayList<>();
    typeList.add(Schema.create(Schema.Type.NULL));
    typeList.addAll(Arrays.asList(types).stream().map(Schema::create).collect(Collectors.toList()));

    Schema unionSchema = Schema.createUnion(typeList);
    return new Schema.Field(name, unionSchema, null, null, Schema.Field.Order.ASCENDING);
  }

  public static Schema.Field createArrayFieldSchema(String name, Schema elementType, String... aliases) {
    return addAliases(new Schema.Field(name, Schema.createArray(elementType), null, null, Schema.Field.Order.ASCENDING),
        aliases);
  }

  public static Schema.Field createMapFieldSchema(String name, Schema valueType, String... aliases) {
    return addAliases(new Schema.Field(name, Schema.createMap(valueType), null, null, Schema.Field.Order.ASCENDING),
        aliases);
  }

  public static Schema createFixedSchema(String name, int size) {
    return Schema.createFixed(name, "", "com.adpilot.utils.generated.avro", size);
  }

  public static Schema createEnumSchema(String name, String[] ordinals) {
    return Schema.createEnum(name, "", "com.adpilot.utils.generated.avro", Arrays.asList(ordinals));
  }

  public static Schema createUnionSchema(Schema... schemas) {
    List<Schema> typeList = new ArrayList<>();
    typeList.add(Schema.create(Schema.Type.NULL));
    typeList.addAll(Arrays.asList(schemas));

    return Schema.createUnion(typeList);
  }

  public static Schema.Field addAliases(Schema.Field field, String... aliases) {
    if (aliases != null) {
      Arrays.asList(aliases).forEach(field::addAlias);
    }

    return field;
  }

  public static <T extends GenericContainer> Decoder genericDataAsDecoder(T data) {
    return genericDataAsDecoder(data, data.getSchema());
  }

  public static <T> Decoder genericDataAsDecoder(T data, Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null);

    try {
      GenericDatumWriter<T> writer = new GenericDatumWriter<>(schema);
      writer.write(data, binaryEncoder);
      binaryEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray(), null);
  }

  public static <T extends SpecificRecord> Decoder specificDataAsDecoder(T record) {
    return specificDataAsDecoder(record, record.getSchema());
  }

  public static <T> Decoder specificDataAsDecoder(T record, Schema schema) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder binaryEncoder = AvroCompatibilityHelper.newBinaryEncoder(baos, true, null);

    try {
      SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
      writer.write(record, binaryEncoder);
      binaryEncoder.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return DecoderFactory.defaultFactory().createBinaryDecoder(baos.toByteArray(), null);
  }

  public static <T> T specificDataFromDecoder(Schema writerSchema, Decoder decoder) {
    SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema);
    try {
      return datumReader.read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static File getCodeGenDirectory() throws IOException {
    Path codeGenOutput = Paths.get("./src/codegen/java/");
    File dir;
    if (Files.notExists(codeGenOutput)) {
      dir = Files.createDirectories(codeGenOutput).toFile();
    } else {
      dir = codeGenOutput.toFile();
    }
    return dir;
  }

  public static void setField(SpecificRecord sr, String fieldName, Object value) {
    Class srClass = sr.getClass();
    try {
      Field field = srClass.getField(fieldName);
      field.set(sr, value);
    } catch (NoSuchFieldException e) {
      String methodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
      Class[] paramClasses = new Class[1];
      try {
        if (value instanceof List) {
          paramClasses[0] = List.class;
        } else if (value instanceof Map) {
          paramClasses[0] = Map.class;
        } else if (value instanceof String) {
          paramClasses[0] = CharSequence.class;
        } else if (value instanceof ByteBuffer) {
          paramClasses[0] = ByteBuffer.class;
        } else {
          paramClasses[0] = value.getClass();
        }
        Method method;
        try {
          method = srClass.getMethod(methodName, paramClasses);
        } catch (NoSuchMethodException ex) {
          // In case it's a union type, then the signature would contain an Object parameter... so we try that
          paramClasses[0] = Object.class;
          try {
            method = srClass.getMethod(methodName, paramClasses);
          } catch (NoSuchMethodException ex2) {
            if (Arrays.stream(value.getClass().getFields()).anyMatch(field -> field.getName().equals("TYPE")) &&
                ((Class) value.getClass().getField("TYPE").get(null)).isPrimitive()) {
              // If a boxed primitive failed, we'll try to look for the actual primitive instead
              paramClasses[0] = (Class) value.getClass().getField("TYPE").get(null);
              try {
                method = srClass.getMethod(methodName, paramClasses);
              } catch (NoSuchMethodException ex3) {
                throw ex;
              }
            } else {
              throw ex;
            }
          }
        }
        Object[] params = new Object[1];
        params[0] = value;
        method.invoke(sr, params);
      } catch (NoSuchMethodException ex) {
        String fields = Arrays.stream(srClass.getMethods())
            .filter(method -> method.getName().startsWith("set"))
            .map(method ->
                method.getName() + "(" +
                    Arrays.stream(method.getParameterTypes())
                        .map(c -> c.getSimpleName())
                        .collect(Collectors.joining(", "))
                    + ")")
            .collect(Collectors.joining(",\n"));
        String methodSignature = methodName + "(" + Arrays.stream(paramClasses)
            .map(c -> c.getSimpleName())
            .collect(Collectors.joining(", ")) + ")";
        throw new RuntimeException("NoSuchMethodException while looking for " + methodSignature + ". Available setters on " + srClass.getSimpleName() + ": " + fields, ex);
      } catch (IllegalAccessException|InvocationTargetException|NoSuchFieldException ex) {
        throw new RuntimeException(ex);
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static Object getField(SpecificRecord sr, String fieldName) {
    Class srClass = sr.getClass();
    try {
      Field field = srClass.getField(fieldName);
      return field.get(sr);
    } catch (NoSuchFieldException e) {
      String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
      try {
        Method method = srClass.getMethod(methodName, new Class[0]);
        return method.invoke(sr, new Object[0]);
      } catch (IllegalAccessException|InvocationTargetException|NoSuchMethodException ex) {
        throw new RuntimeException(ex);
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
