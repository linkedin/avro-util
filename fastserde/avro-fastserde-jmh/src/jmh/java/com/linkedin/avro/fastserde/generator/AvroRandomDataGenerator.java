/*
   Copyright 2018 Confluent Inc.

   Licensed under the Apache License, Version 2.0 (the "License"); you may not
   use this file except in compliance with the License. You may obtain a copy of
   the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   License for the specific language governing permissions and limitations under
   the License.
 */

package com.linkedin.avro.fastserde.generator;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;


/**
 * Generates Java objects according to an {@link Schema Avro Schema}.
 */
@SuppressWarnings("WeakerAccess")
@Deprecated //functionality to be ported over to RandomRecordGenerator in "core" helper
public class AvroRandomDataGenerator {

  /**
   * The name to use for the top-level JSON property when specifying ARG-specific attributes.
   */
  public static final String ARG_PROPERTIES_PROP = "arg.properties";

  /**
   * The name of the attribute for specifying length of array. Can be given as either
   * an integral number or an object with at least one of {@link #LENGTH_PROP_MIN} or
   * {@link #LENGTH_PROP_MAX} specified.
   */
  public static final String ARRAY_LENGTH_PROP = "arrayLength";
  /**
   * The name of the attribute for specifying length of string. Can be given as either
   * an integral number or an object with at least one of {@link #LENGTH_PROP_MIN} or
   * {@link #LENGTH_PROP_MAX} specified.
   */
  public static final String STRING_LENGTH_PROP = "stringLength";
  /**
   * The name of the attribute for specifying length of bytes. Can be given as either
   * an integral number or an object with at least one of {@link #LENGTH_PROP_MIN} or
   * {@link #LENGTH_PROP_MAX} specified.
   */
  public static final String BYTES_LENGTH_PROP = "bytesLength";
  /**
   * The name of the attribute for specifying length of Map. Can be given as either
   * an integral number or an object with at least one of {@link #LENGTH_PROP_MIN} or
   * {@link #LENGTH_PROP_MAX} specified.
   */
  public static final String MAP_LENGTH_PROP = "mapLength";
  /**
   * The name of the attribute for specifying the minimum length a generated value should have.
   * Must be given as an integral number greater than or equal to zero.
   */
  public static final String LENGTH_PROP_MIN = "min";
  /**
   * The name of the attribute for specifying the maximum length a generated value should have.
   * Must be given as an integral number strictly greater than the value given for
   * {@link #LENGTH_PROP_MIN}, or strictly greater than zero if none is specified.
   */
  public static final String LENGTH_PROP_MAX = "max";

  /**
   * The name of the attribute for specifying a prefix that generated values should begin with. Will
   * be prepended to the beginning of any string values generated. Can be used in conjunction with
   * {@link #SUFFIX_PROP}.
   */
  public static final String PREFIX_PROP = "prefix";
  /**
   * The name of the attribute for specifying a suffix that generated values should end with. Will
   * be appended to the end of any string values generated. Can be used in conjunction with
   * {@link #PREFIX_PROP}.
   */
  public static final String SUFFIX_PROP = "suffix";

  /**
   * The name of the attribute for specifying a possible range of values for numeric types. Must be
   * given as an object.
   */
  public static final String RANGE_PROP = "range";
  /**
   * The name of the attribute for specifying the (inclusive) minimum value in a range. Must be
   * given as a numeric type that is integral if the given schema is as well.
   */
  public static final String RANGE_PROP_MIN = "min";
  /**
   * The name of the attribute for specifying the (exclusive) maximum value in a range. Must be
   * given as a numeric type that is integral if the given schema is as well.
   */
  public static final String RANGE_PROP_MAX = "max";

  /**
   * The name of the attribute for specifying the likelihood that the value true is generated for a
   * boolean schema. Must be given as a floating type in the range [0.0, 1.0].
   */
  public static final String ODDS_PROP = "odds";

  private final Schema topLevelSchema;
  private final Random random;

  /**
   * Creates a generator out of an already-parsed {@link Schema}.
   * @param topLevelSchema The schema to generate values for.
   * @param random The object to use for generating randomness when producing values.
   */
  public AvroRandomDataGenerator(Schema topLevelSchema, Random random) {
    this.topLevelSchema = topLevelSchema;
    this.random = random;
  }

  /**
   * @return The schema that the generator produces values for.
   */
  public Schema schema() {
    return topLevelSchema;
  }

  /**
   * Generate an object that matches the given schema and its specified properties.
   * @return An object whose type corresponds to the top-level schema as follows:
   * <table summary="Schema Type-to-Java class specifications">
   *   <tr>
   *     <th>Schema Type</th>
   *     <th>Java Class</th>
   *   </tr>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#ARRAY ARRAY}</td>
   *     <td>{@link Collection}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#BOOLEAN BOOLEAN}</td>
   *     <td>{@link Boolean}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#BYTES BYTES}</td>
   *     <td>{@link ByteBuffer}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#DOUBLE DOUBLE}</td>
   *     <td>{@link Double}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#ENUM ENUM}</td>
   *     <td>{@link GenericEnumSymbol}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#FIXED FIXED}</td>
   *     <td>{@link GenericFixed}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#FLOAT FLOAT}</td>
   *     <td>{@link Float}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#INT INT}</td>
   *     <td>{@link Integer}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#LONG LONG}</td>
   *     <td>{@link Long}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#MAP MAP}</td>
   *     <td>
   *       {@link Map}&lt;{@link String}, V&gt; where V is the corresponding Java class for the
   *       Avro map's values
   *     </td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#NULL NULL}</td>
   *     <td>{@link Object} (but will always be null)</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#RECORD RECORD}</td>
   *     <td>{@link GenericRecord}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#STRING STRING}</td>
   *     <td>{@link String}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#UNION UNION}</td>
   *     <td>
   *       The corresponding Java class for whichever schema is chosen to be generated out of the
   *       ones present in the given Avro union.
   *     </td>
   * </table>
   */
  public Object generate(Map propertiesProp) {
    return generateObject(topLevelSchema, propertiesProp);
  }

  private Object generateObject(Schema schema,  Map propertiesProp) {
    switch (schema.getType()) {
      case ARRAY:
        return generateArray(schema, propertiesProp);
      case BOOLEAN:
        return generateBoolean(propertiesProp);
      case BYTES:
        return generateBytes(propertiesProp);
      case DOUBLE:
        return generateDouble(propertiesProp);
      case ENUM:
        return generateEnumSymbol(schema);
      case FIXED:
        return generateFixed(schema);
      case FLOAT:
        return generateFloat(propertiesProp);
      case INT:
        return generateInt(propertiesProp);
      case LONG:
        return generateLong(propertiesProp);
      case MAP:
        return generateMap(schema, propertiesProp);
      case NULL:
        return generateNull();
      case RECORD:
        return generateRecord(schema, propertiesProp);
      case STRING:
        return generateString(propertiesProp);
      case UNION:
        return generateUnion(schema, propertiesProp);
      default:
        throw new RuntimeException("Unrecognized schema type: " + schema.getType());
    }
  }

  private Collection<Object> generateArray(Schema schema, Map propertiesProp) {
    int length = getLengthBounds(propertiesProp, ARRAY_LENGTH_PROP).random();
    Collection<Object> result = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      result.add(generateObject(schema.getElementType(), propertiesProp));
    }
    return result;
  }

  private Boolean generateBoolean(Map propertiesProp) {
    Double odds = getDecimalNumberField(ARG_PROPERTIES_PROP, ODDS_PROP, propertiesProp);
    if (odds == null) {
      return random.nextBoolean();
    } else {
      if (odds < 0.0 || odds > 1.0) {
        throw new RuntimeException(String.format(
            "%s property must be in the range [0.0, 1.0]",
            ODDS_PROP
        ));
      }
      return random.nextDouble() < odds;
    }
  }

  private ByteBuffer generateBytes(Map propertiesProp) {
    byte[] bytes = new byte[getLengthBounds(propertiesProp.get(BYTES_LENGTH_PROP), BYTES_LENGTH_PROP).random()];
    random.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  private Double generateDouble(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Double rangeMinField = getDecimalNumberField(RANGE_PROP, RANGE_PROP_MIN, rangeProps);
        Double rangeMaxField = getDecimalNumberField(RANGE_PROP, RANGE_PROP_MAX, rangeProps);
        double rangeMin = rangeMinField != null ? rangeMinField : -1 * Double.MAX_VALUE;
        double rangeMax = rangeMaxField != null ? rangeMaxField : Double.MAX_VALUE;
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + (random.nextDouble() * (rangeMax - rangeMin));
      } else {
        throw new RuntimeException(String.format(
            "%s property must be an object",
            RANGE_PROP
        ));
      }
    }
    return random.nextDouble();
  }

  private GenericEnumSymbol generateEnumSymbol(Schema schema) {
    List<String> enums = schema.getEnumSymbols();
    return AvroCompatibilityHelper.newEnumSymbol(schema, enums.get(random.nextInt(enums.size())));
  }

  private GenericFixed generateFixed(Schema schema) {
    byte[] bytes = new byte[schema.getFixedSize()];
    random.nextBytes(bytes);
    return AvroCompatibilityHelper.newFixedField(schema, bytes);
  }

  private Float generateFloat(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Float rangeMinField = getFloatNumberField(
            RANGE_PROP,
            RANGE_PROP_MIN,
            rangeProps
        );
        Float rangeMaxField = getFloatNumberField(
            RANGE_PROP,
            RANGE_PROP_MAX,
            rangeProps
        );
        float rangeMin = Optional.ofNullable(rangeMinField).orElse(-1 * Float.MAX_VALUE);
        float rangeMax = Optional.ofNullable(rangeMaxField).orElse(Float.MAX_VALUE);
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + (random.nextFloat() * (rangeMax - rangeMin));
      }
    }
    return random.nextFloat();
  }

  private Integer generateInt(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Integer rangeMinField = getIntegerNumberField(RANGE_PROP, RANGE_PROP_MIN, rangeProps);
        Integer rangeMaxField = getIntegerNumberField(RANGE_PROP, RANGE_PROP_MAX, rangeProps);
        int rangeMin = Optional.ofNullable(rangeMinField).orElse(Integer.MIN_VALUE);
        int rangeMax = Optional.ofNullable(rangeMaxField).orElse(Integer.MAX_VALUE);
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + ((int) (random.nextDouble() * (rangeMax - rangeMin)));
      }
    }
    return random.nextInt();
  }

  private Long generateLong(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Long rangeMinField = getIntegralNumberField(RANGE_PROP, RANGE_PROP_MIN, rangeProps);
        Long rangeMaxField = getIntegralNumberField(RANGE_PROP, RANGE_PROP_MAX, rangeProps);
        long rangeMin = Optional.ofNullable(rangeMinField).orElse(Long.MIN_VALUE);
        long rangeMax = Optional.ofNullable(rangeMaxField).orElse(Long.MAX_VALUE);
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + (((long) (random.nextDouble() * (rangeMax - rangeMin))));
      }
    }
    return random.nextLong();
  }

  private Map<String, Object> generateMap(Schema schema, Map propertiesProp) {
    Map<String, Object> result = new HashMap<>();
    int length = getLengthBounds(propertiesProp, MAP_LENGTH_PROP).random();
    for (int i = 0; i < length; i++) {
      result.put(generateRandomString(5), generateObject(schema.getValueType(), propertiesProp));
    }
    return result;
  }

  private Object generateNull() {
    return null;
  }

  private GenericRecord generateRecord(Schema schema, Map propertyProps) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field, generateObject(field.schema(), propertyProps));
    }
    return builder.build();
  }

  private String generateRandomString(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) random.nextInt(128);
    }
    return new String(bytes, StandardCharsets.US_ASCII);
  }

  private String generateString(Map propertiesProp) {

    String result = generateRandomString(getLengthBounds(propertiesProp, STRING_LENGTH_PROP).random());

    return  prefixAndSuffixString(result, propertiesProp);
  }

  private String prefixAndSuffixString(String result, Map propertiesProp) {
    Object prefixProp = propertiesProp.get(PREFIX_PROP);
    if (prefixProp != null && !(prefixProp instanceof String)) {
      throw new RuntimeException(String.format("%s property must be a string", PREFIX_PROP));
    }
    String prefix = prefixProp != null ? (String) prefixProp : "";

    Object suffixProp = propertiesProp.get(SUFFIX_PROP);
    if (suffixProp != null && !(suffixProp instanceof String)) {
      throw new RuntimeException(String.format("%s property must be a string", SUFFIX_PROP));
    }
    String suffix = suffixProp != null ? (String) suffixProp : "";

    return prefix + result + suffix;
  }

  // try to generate not null field for Union field
  private Object generateUnion(Schema schema, Map propertiesProp) {
    List<Schema> schemas = schema.getTypes();
    int count = 0;
    Object generatedUnionObject = null;
    while (count < 5) {
      generatedUnionObject = generateObject(schemas.get(random.nextInt(schemas.size())), propertiesProp);
      if (generatedUnionObject != null) {
        break;
      }
      count++;
    }
    return generatedUnionObject;
  }

  private LengthBounds getLengthBounds(Map propertiesProp, String lengthKey) {
    return getLengthBounds(propertiesProp.get(lengthKey), lengthKey);
  }

  private LengthBounds getLengthBounds(Object lengthProp, String lengthKey) {
    if (lengthProp == null) {
      return new LengthBounds();
    } else if (lengthProp instanceof Integer) {
      Integer length = (Integer) lengthProp;
      if (length < 0) {
        throw new RuntimeException(String.format(
            "when given as integral number, %s property cannot be negative",
            lengthKey
        ));
      }
      return new LengthBounds(length);
    } else if (lengthProp instanceof Map) {
      Map lengthProps = (Map) lengthProp;
      Integer minLength = getIntegerNumberField(lengthKey, LENGTH_PROP_MIN, lengthProps);
      Integer maxLength = getIntegerNumberField(lengthKey, LENGTH_PROP_MAX, lengthProps);
      if (minLength == null && maxLength == null) {
        throw new RuntimeException(String.format(
            "%s property must contain at least one of '%s' or '%s' fields when given as object",
            lengthKey,
            LENGTH_PROP_MIN,
            LENGTH_PROP_MAX
        ));
      }
      minLength = minLength != null ? minLength : 0;
      maxLength = maxLength != null ? maxLength : Integer.MAX_VALUE;
      if (minLength < 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property cannot be negative",
            LENGTH_PROP_MIN,
            lengthKey
        ));
      }
      if (maxLength <= minLength) {
        throw new RuntimeException(String.format(
            "%s field must be strictly greater than %s field for %s property",
            LENGTH_PROP_MAX,
            LENGTH_PROP_MIN,
            lengthKey
        ));
      }
      return new LengthBounds(minLength, maxLength);
    } else {
      throw new RuntimeException(String.format(
          "%s property must either be an integral number or an object, was %s instead",
          lengthKey,
          lengthProp.getClass().getName()
      ));
    }
  }

  private Integer getIntegerNumberField(String property, String field, Map propsMap) {
    Long result = getIntegralNumberField(property, field, propsMap);
    if (result != null && (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE)) {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be a valid int for int schemas",
          field,
          property
      ));
    }
    return result != null ? result.intValue() : null;
  }

  private Long getIntegralNumberField(String property, String field, Map propsMap) {
    Object result = propsMap.get(field);
    if (result == null || result instanceof Long) {
      return (Long) result;
    } else if (result instanceof Integer) {
      return ((Integer) result).longValue();
    } else {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be an integral number, was %s instead",
          field,
          property,
          result.getClass().getName()
      ));
    }
  }

  private Float getFloatNumberField(String property, String field, Map propsMap) {
    Double result = getDecimalNumberField(property, field, propsMap);
    if (result != null && (result > Float.MAX_VALUE || result < -1 * Float.MAX_VALUE)) {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be a valid float for float schemas",
          field,
          property
      ));
    }
    return result != null ? result.floatValue() : null;
  }

  private Double getDecimalNumberField(String property, String field, Map propsMap) {
    Object result = propsMap.get(field);
    if (result == null || result instanceof Double) {
      return (Double) result;
    } else if (result instanceof Float) {
      return ((Float) result).doubleValue();
    } else if (result instanceof Integer) {
      return ((Integer) result).doubleValue();
    } else if (result instanceof Long) {
      return ((Long) result).doubleValue();
    } else {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be a number, was %s instead",
          field,
          property,
          result.getClass().getName()
      ));
    }
  }

  private class LengthBounds {
    public static final int DEFAULT_MIN = 8;
    public static final int DEFAULT_MAX = 16;

    private final int min;
    private final int max;

    public LengthBounds(int min, int max) {
      this.min = min;
      this.max = max;
    }

    public LengthBounds(int exact) {
      this(exact, exact + 1);
    }

    public LengthBounds() {
      this(DEFAULT_MIN, DEFAULT_MAX);
    }

    public int random() {
      return min + random.nextInt(max - min);
    }
  }

}
