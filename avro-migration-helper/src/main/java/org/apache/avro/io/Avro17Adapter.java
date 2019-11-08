/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.linkedin.avro.compatibility.SchemaParseResult;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;


public class Avro17Adapter extends AbstractAvroAdapter {
  private static final String PARSER_INVOCATION_START = "new org.apache.avro.Schema.Parser().parse(";
  private static final Pattern PARSER_INVOCATION_PATTERN = Pattern.compile(Pattern.quote(PARSER_INVOCATION_START) + "\"(.*)\"\\);([\r\n]+)");
  private static final String CSV_SEPARATOR = "(?<!\\\\)\",\""; //require the 1st " not be preceded by a \
  private static final Pattern BUILDER_START_PATTERN = Pattern.compile("/\\*\\*([\\s*])*Creates a new \\w+ RecordBuilder");
  private static final Pattern AVROGENERATED_PATTERN = Pattern.compile(Pattern.quote("@org.apache.avro.specific.AvroGenerated"));
  private static final String COMMENTED_OUT_AVROGENERATED = Matcher.quoteReplacement("// @org.apache.avro.specific.AvroGenerated");
  private static final Pattern SUPER_CTR_BYTES_PATTERN = Pattern.compile(Pattern.quote("super(bytes);"));
  private static final String BYTES_INVOCATION = Matcher.quoteReplacement("super();\n    bytes(bytes);");
  private static final Pattern EXTERNALIZABLE_SUPPORT_START_PATTERN = Pattern.compile(Pattern.quote("private static final org.apache.avro.io.DatumWriter"));
  private static final Pattern WRITE_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void writeExternal(java.io.ObjectOutput out)"));
  private static final String WRITE_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void writeExternal(java.io.ObjectOutput out)");
  private static final Pattern READ_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void readExternal(java.io.ObjectInput in)"));
  private static final String READ_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void readExternal(java.io.ObjectInput in)");
  private static final Pattern CREATE_ENCODER_INVOCATION = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getEncoder(out)"));
  private static final String CREATE_ENCODER_VIA_HELPER = Matcher.quoteReplacement(AvroCompatibilityHelper.class.getCanonicalName() + ".newBinaryEncoder(out)");
  private static final Pattern CREATE_DECODER_INVOCATION = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getDecoder(in)"));
  private static final String CREATE_DECODER_VIA_HELPER = Matcher.quoteReplacement(AvroCompatibilityHelper.class.getCanonicalName() + ".newBinaryDecoder(in)");
  private static final Pattern CATCH_EXCEPTION_PATTERN = Pattern.compile(Pattern.quote("catch (Exception e)"));
  private static final String CATCH_FULLY_QUALIFIED_EXCEPTION = Matcher.quoteReplacement("catch (java.lang.Exception e)");

  private final Object _encoderFactory;
  private final Method _encoderFactoryBinaryEncoderMethod;
  private final Constructor<?> _schemaParserCtr;
  private final Method _setValidateMethod;
  private final Method _setValidateDefaultsMethod; //in 1.8+
  private final Method _parseMethod;
  private final Method _addTypesMethod;
  private final Method _getTypesMethod;
  private final Method _toParsingFormMethod;
  private final Method _newInstanceMethod;
  private final Method _getDefaultValueMethod;

  //compiler-related
  private boolean _compilerSupported; //defaults to false
  private Object _publicFieldVisibilityEnumInstance;
  private Method _setFieldVisibilityMethod;
  private Object _charSequenceStringTypeEnumInstance;
  private Method _setStringTypeMethod;

  public Avro17Adapter() throws Exception {
    super(
        GenericData.EnumSymbol.class.getConstructor(Schema.class, String.class),
        GenericData.Fixed.class.getConstructor(Schema.class, byte[].class)
    );
    _encoderFactory = Class.forName("org.apache.avro.io.EncoderFactory").getDeclaredMethod("get").invoke(null);
    _encoderFactoryBinaryEncoderMethod = _encoderFactory.getClass().getMethod("binaryEncoder", OutputStream.class, BinaryEncoder.class);

    Class<?> schemaParserClass = Class.forName("org.apache.avro.Schema$Parser");
    _schemaParserCtr = schemaParserClass.getConstructor();
    _setValidateMethod = schemaParserClass.getDeclaredMethod("setValidate", boolean.class);
    AvroVersion runtimeVersion = AvroCompatibilityHelper.getRuntimeAvroVersion();
    if (runtimeVersion.laterThan(AvroVersion.AVRO_1_7)) {
      _setValidateDefaultsMethod = schemaParserClass.getDeclaredMethod("setValidateDefaults", boolean.class);
    } else {
      _setValidateDefaultsMethod = null;
    }
    _parseMethod = schemaParserClass.getDeclaredMethod("parse", String.class);
    _addTypesMethod = schemaParserClass.getDeclaredMethod("addTypes", Map.class);
    _getTypesMethod = schemaParserClass.getDeclaredMethod("getTypes");

    Class<?> schemaNormalizationClass = Class.forName("org.apache.avro.SchemaNormalization");
    _toParsingFormMethod = schemaNormalizationClass.getMethod("toParsingForm", Schema.class);

    _newInstanceMethod = SpecificData.class.getMethod("newInstance", Class.class, Schema.class);
    _getDefaultValueMethod = SpecificData.class.getMethod("getDefaultValue", Schema.Field.class);

    tryInitializeCompilerFields();
  }

  private void tryInitializeCompilerFields() throws Exception {
    //compiler was moved out into a separate jar in modern avro, so compiler functionality is optional
    try {
      Class<?> compilerClass = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler");
      _specificCompilerCtr = compilerClass.getConstructor(Schema.class);
      _compilerEnqueueMethod = compilerClass.getDeclaredMethod("enqueue", Schema.class);
      _compilerEnqueueMethod.setAccessible(true); //its normally private
      _compilerCompileMethod = compilerClass.getDeclaredMethod("compile");
      _compilerCompileMethod.setAccessible(true); //package-protected
      Class<?> outputFileClass = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler$OutputFile");
      _outputFilePathField = outputFileClass.getDeclaredField("path");
      _outputFilePathField.setAccessible(true);
      _outputFileContentsField = outputFileClass.getDeclaredField("contents");
      _outputFileContentsField.setAccessible(true);
      Class<?> fieldVisibilityEnum = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler$FieldVisibility");
      Field publicVisibilityField = fieldVisibilityEnum.getDeclaredField("PUBLIC");
      _publicFieldVisibilityEnumInstance = publicVisibilityField.get(null);
      _setFieldVisibilityMethod = compilerClass.getDeclaredMethod("setFieldVisibility", fieldVisibilityEnum);
      Class<?> fieldTypeEnum = Class.forName("org.apache.avro.generic.GenericData$StringType");
      Field charSequenceField = fieldTypeEnum.getDeclaredField("CharSequence");
      _charSequenceStringTypeEnumInstance = charSequenceField.get(null);
      _setStringTypeMethod = compilerClass.getDeclaredMethod("setStringType", fieldTypeEnum);
      _compilerSupported = true;
    } catch (Exception e) {
      _compilerSupported = false;
      //ignore
    }
  }

  @Override
  public BinaryEncoder newBinaryEncoder(OutputStream out) {
    try {
      return (BinaryEncoder) _encoderFactoryBinaryEncoderMethod.invoke(_encoderFactory, out, null);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
    try {
      return (GenericData.EnumSymbol) _enumSymbolCtr.newInstance(avroSchema, enumValue);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
    try {
      return (GenericData.Fixed) _fixedCtr.newInstance(ofType, contents);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Object newInstance(Class c, Schema s) {
    try {
      return _newInstanceMethod.invoke(null, c, s);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Object getDefaultValue(Schema.Field field) {
    try {
      return _getDefaultValueMethod.invoke(SpecificData.get(), field);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public SchemaParseResult parse(String schemaJson, Collection<Schema> known) {
    try {
      Object schemaParser = _schemaParserCtr.newInstance();
      _setValidateMethod.invoke(schemaParser, Boolean.TRUE);
      if (_setValidateDefaultsMethod != null) {
        _setValidateDefaultsMethod.invoke(schemaParser, Boolean.TRUE);
      }

      if (known != null) {
        Map<String, Schema> types = new HashMap<>(known.size());
        for (Schema s : known) {
          types.put(s.getFullName(), s);
        }
        _addTypesMethod.invoke(schemaParser, types);
      }

      Schema parsed = (Schema) _parseMethod.invoke(schemaParser, schemaJson);
      //noinspection unchecked
      Map<String, Schema> allKnown = (Map<String, Schema>) _getTypesMethod.invoke(schemaParser);
      return new SchemaParseResult(parsed, allKnown);
    } catch (RuntimeException e) {
      throw e; //pass-through
    } catch (InvocationTargetException e) {
      Throwable underlying = e.getCause();
      if (underlying instanceof RuntimeException) {
        throw (RuntimeException) underlying;
      }
      throw new IllegalStateException(e);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion minSupportedRuntimeVersion) {
    if (!_compilerSupported) {
      throw new UnsupportedOperationException("avro compiler jar was not found on classpath (was made an independent jar in modern avro)");
    }
    return super.compile(toCompile, minSupportedRuntimeVersion);
  }

  @Override
  protected Object getCompilerInstance(Schema firstSchema, AvroVersion compatibilityTarget) throws Exception {
    Object instance = super.getCompilerInstance(firstSchema, compatibilityTarget);
    if (compatibilityTarget == AvroVersion.AVRO_1_4) {
      //configure compiler to be as avro-1.4 compliant as possible within the avro compiler API
      _setFieldVisibilityMethod.invoke(instance, _publicFieldVisibilityEnumInstance); //make fields public
      //configure string types to generate as CharSequence (because thats the only way 1.4 does this)
      _setStringTypeMethod.invoke(instance, _charSequenceStringTypeEnumInstance);
    }
    return instance;
  }

  @Override
  protected List<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroCodegenOutput, AvroVersion compatibilityLevel) {
    List<AvroGeneratedSourceCode> transformed = new ArrayList<>(avroCodegenOutput.size());

    Function<String, String> makeCompatible;
    switch (compatibilityLevel) {
      case AVRO_1_8:
        makeCompatible = Function.identity();
        break;
      case AVRO_1_7:
        makeCompatible = Avro17Adapter::make17Compatible;
        break;
      case AVRO_1_4:
        makeCompatible = Avro17Adapter::make14Compatible;
        break;
      default:
        throw new UnsupportedOperationException("unhandled: " + compatibilityLevel);
    }

    for (AvroGeneratedSourceCode generated : avroCodegenOutput) {
      String fixed = fixKnownIssues(makeCompatible.apply(generated.getContents()));
      transformed.add(new AvroGeneratedSourceCode(generated.getPath(), fixed));
    }

    return transformed;
  }

  @Override
  public String toParsingForm(Schema s) {
    try {
      return (String) _toParsingFormMethod.invoke(null, s);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * given a java class generated by modern avro, make the code compatible with an avro 1.4 runtime.
   * @param avroGenerated a class generated by modern avro
   * @return avro-1.4 compatible version of the input class
   */
  private static String make14Compatible(String avroGenerated) {
    String result = make17Compatible(avroGenerated);

    result = commentOutAvroGeneratedAnnotation(result);

    result = makeSchemaDollarBackwardsCompatible(result);

    result = removeBuilderSupport(result);

    result = fixByteArrayConstructor(result);

    return result;
  }

  private static String make17Compatible(String avroGenerated) {
    return makeExternalizableSupportBackwardsCompatible(avroGenerated);
  }

  /**
   * comment out annotation that only exists in avro 1.7
   * @param code specific record code generated by avro
   * @return generated code with the AvroGenerated annotation commented out
   */
  static String commentOutAvroGeneratedAnnotation(String code) {
    return AVROGENERATED_PATTERN.matcher(code).replaceAll(COMMENTED_OUT_AVROGENERATED);
  }

  /**
   * makes changes to the SCHEMA$ field in generated specific classes to make them avro-1.4 compatible
   * @param code specific record code generated by avro
   * @return generated code with SCHEMA$ declaration made avro-1.4 compatible
   */
  static String makeSchemaDollarBackwardsCompatible(String code) {
    String result = code;

    //issues we have with avro's generated SCHEMA$ field:
    //1. modern avro uses Schema.Parser().parse() whereas 1.4 doesnt have it. 1.7 /1.8 still has Schema.parse() (just deprecated)
    //   so we resort to using that for compatibility
    //2. the java language has a limitation where string literals cannot be over 64K long (this is way less than 64k
    //   characters for complicated unicode) - see AVRO-1316. modern avro has a vararg parse(String...) to get around this
    //   we will need to convert that into new StringBuilder().append().append()...toString()
    Matcher matcher = PARSER_INVOCATION_PATTERN.matcher(result); //group 1 would be the args to parse(), group 2 would be some line break
    if (matcher.find()) {
      String argsStr = result.substring(matcher.start(1), matcher.end(1));
      String lineBreak = matcher.group(2);
      String[] varArgs = argsStr.split(CSV_SEPARATOR);
      String singleArg;
      if (varArgs.length > 1) {
        StringBuilder argBuilder = new StringBuilder(argsStr.length());
        argBuilder.append("new StringBuilder()");
        Arrays.stream(varArgs).forEach(literal -> {
          argBuilder.append(".append(\"").append(literal).append("\")");
        });
        argBuilder.append(".toString()");
        singleArg = argBuilder.toString();
      } else {
        singleArg = "\"" + varArgs[0] + "\"";
      }
      result = matcher.replaceFirst(Matcher.quoteReplacement("org.apache.avro.Schema.parse(" + singleArg + ");") + lineBreak);
    }

    return result;
  }

  /**
   * removes the generated builder class and related methods from an avro-generated specific record.
   * the builder relies on a base class that does not exist in avro 1.4
   * @param code specific record code generated by avro
   * @return generated code without builder support
   */
  static String removeBuilderSupport(String code) {
    String result = code;

    Matcher matcher = BUILDER_START_PATTERN.matcher(result);
    if (matcher.find()) {
      int builderSupportStart = matcher.start();
      Matcher endMatcher = EXTERNALIZABLE_SUPPORT_START_PATTERN.matcher(result);
      if (endMatcher.find()) {
        //avro 1.8 has externalizable support (which we must keep) after the builder support section
        int externalizableSupportStart = endMatcher.start();
        if (externalizableSupportStart <= builderSupportStart) {
          throw new IllegalStateException("unable to properly locate builder vs externalizable support code");
        }
        result = result.substring(0, builderSupportStart) + result.substring(externalizableSupportStart);
      } else {
        //builder support is at the end of the file
        result = result.substring(0, builderSupportStart) + "\n}";
      }
    }

    return result;
  }

  /**
   * fixes the byte[]-accepting constructor in some avro-generated classes to work under avro 1.4
   * @param code specific record code generated by avro
   * @return generated code with avro-1.4 compatible byte[] handling
   */
  static String fixByteArrayConstructor(String code) {
    return SUPER_CTR_BYTES_PATTERN.matcher(code).replaceAll(BYTES_INVOCATION);
  }

  /**
   * makes the externalizable support code generated by avro 1.8 compile/work with older avro
   * @param code specific record generated by avro
   * @return generated code with externalizable support made backwards compatible
   */
  static String makeExternalizableSupportBackwardsCompatible(String code) {
    String result = code;

    //strip out the "@Override" annotations from Externalizable methods because the parent class
    //(SpecificFixed) is not Externalizable in older avro
    result = WRITE_EXTERNAL_SIGNATURE.matcher(result).replaceAll(WRITE_EXTERNAL_WITHOUT_OVERRIDE);
    result = READ_EXTERNAL_SIGNATURE.matcher(result).replaceAll(READ_EXTERNAL_WITHOUT_OVERRIDE);

    //next up - 1.8 Externalizable support relies on utility code that doesnt exist in <= 1.7
    //so we switch it to use the helper
    result = CREATE_ENCODER_INVOCATION.matcher(result).replaceAll(CREATE_ENCODER_VIA_HELPER);
    result = CREATE_DECODER_INVOCATION.matcher(result).replaceAll(CREATE_DECODER_VIA_HELPER);

    return result;
  }

  /**
   * given a java class generated by modern avro, fix known issues
   * @param avroGenerated java code generated by modern avro
   * @return a version of the input code, with any known issues fixed.
   */
  private static String fixKnownIssues(String avroGenerated) {
    //because who in their right minds would call their class Exception, right? :-(
    //also, this bug has been fixed in upstream avro, but there's been no 1.7.* release with the fix yet
    //see AVRO-1901
    return CATCH_EXCEPTION_PATTERN.matcher(avroGenerated).replaceAll(CATCH_FULLY_QUALIFIED_EXCEPTION);
  }
}
