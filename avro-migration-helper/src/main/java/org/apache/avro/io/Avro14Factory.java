/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import com.linkedin.avro.compatibility.SchemaNormalization;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


public class Avro14Factory extends AbstractAvroFactory {
  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+(.*);");
  private static final Pattern FIXED_SIZE_ANNOTATION_PATTERN = Pattern.compile("@org.apache.avro.specific.FixedSize\\((.*)\\)");
  private static final Pattern FIXED_CLASS_DECL_PATTERN = Pattern.compile("public class (\\w+) extends org.apache\\.avro\\.specific\\.SpecificFixed ");
  private static final Pattern ENUM_CLASS_ANNOTATION_PATTERN = Pattern.compile("public enum (\\w+) ");
  private static final Pattern ENUM_CLASS_DECL_PATTERN = Pattern.compile("public enum (\\w+) \\{\\s*[\\n\\r]\\s*(.*)\\s*[\\n\\r]}");
  private static final Pattern TEMPLATE_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(\\w+)}");
  private static final Pattern COMMENT_PATTERN = Pattern.compile("(//([/\\s]*).*?\\s*$)|(/\\*+\\s*(.*?)\\s*\\*+/)", Pattern.MULTILINE | Pattern.DOTALL);
  private static final String FIXED_CLASS_BODY_TEMPLATE = loadTemplate("SpecificFixedBody.template");
  private static final String FIXED_CLASS_NO_NAMESPACE_BODY_TEMPLATE = loadTemplate("SpecificFixedBodyNoNamespace.template");
  private static final String ENUM_CLASS_BODY_TEMPLATE = loadTemplate("Enum.template");
  private static final String ENUM_CLASS_NO_NAMESPACE_BODY_TEMPLATE = loadTemplate("EnumNoNamespace.template");
  private static final String PARSE_INVOCATION_START = "org.apache.avro.Schema.parse(";
  private static final Pattern PARSE_INVOCATION_PATTERN = Pattern.compile(Pattern.quote(PARSE_INVOCATION_START) + "\"(.*)\"\\);");
  private static final int MAX_STRING_LITERAL_SIZE = 65000; //just under 64k

  private final Constructor _binaryEncoderCtr;
  private final Method _schemaParseMethod;

  public Avro14Factory() throws Exception {
    super(
        GenericData.EnumSymbol.class.getConstructor(String.class),
        GenericData.Fixed.class.getConstructor(byte[].class)
    );
    _binaryEncoderCtr = BinaryEncoder.class.getConstructor(OutputStream.class);
    Class<?> compilerClass = Class.forName("org.apache.avro.specific.SpecificCompiler");
    _specificCompilerCtr = compilerClass.getConstructor(Schema.class);
    _compilerEnqueueMethod = compilerClass.getDeclaredMethod("enqueue", Schema.class);
    _compilerEnqueueMethod.setAccessible(true); //its normally private
    _compilerCompileMethod = compilerClass.getDeclaredMethod("compile");
    _compilerCompileMethod.setAccessible(true); //package-protected
    Class<?> outputFileClass = Class.forName("org.apache.avro.specific.SpecificCompiler$OutputFile");
    _outputFilePathField = outputFileClass.getDeclaredField("path");
    _outputFilePathField.setAccessible(true);
    _outputFileContentsField = outputFileClass.getDeclaredField("contents");
    _outputFileContentsField.setAccessible(true);
    _schemaParseMethod = Schema.class.getDeclaredMethod("parse", String.class);
  }

  @Override
  protected List<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroCodegenOutput, AvroVersion compatibilityLevel) {
    if (compatibilityLevel.ordinal() > AvroVersion.AVRO_1_4.ordinal()) {
      //things like lack of SCHEMA$ field on fixed types is an issue with 1.5+
      //at higher versions even more would be missing (implementation of Externalizable for 1.8, for example)
      throw new IllegalStateException("avro-1.4 generated specific records cannot in general be made compatible with " + compatibilityLevel);
    }
    List<AvroGeneratedSourceCode> transformed = new ArrayList<>(avroCodegenOutput.size());
    String fixed;
    for (AvroGeneratedSourceCode generated : avroCodegenOutput) {
      fixed = generated.getContents();
      fixed = addMissingMethodsToFixedClass(fixed);
      fixed = addSchemaStringToEnumClass(fixed);
      fixed = splitUpBigParseCalls(fixed);
      transformed.add(new AvroGeneratedSourceCode(generated.getPath(), fixed));
    }
    return transformed;
  }

  @Override
  public BinaryEncoder newBinaryEncoder(OutputStream out) {
    try {
      return (BinaryEncoder) _binaryEncoderCtr.newInstance(out);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
    try {
      return (GenericData.EnumSymbol) _enumSymbolCtr.newInstance(enumValue);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
    try {
      return (GenericData.Fixed) _fixedCtr.newInstance((Object) contents);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Schema parse(String schemaJson) {
    try {
      return (Schema) _schemaParseMethod.invoke(null, schemaJson);
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
  public String toParsingForm(Schema s) {
    return SchemaNormalization.toParsingForm(s);
  }

  /**
   * avro 1.4 generates enum classes are incompatible with modern avro. modern avro expects enums to:
   * <ul>
   *   <li>to have a public static final org.apache.avro.Schema SCHEMA$ field (at least avro 1.7)</li>
   *   <li>to have a public Schema getSchema() method (in avro 1.8) that returns the above SCHEMA$</li>
   * </ul>
   * this method introduces these into generated code for enum classes
   * @param code generated code
   * @return if not an enum class returns input. otherwise returns transformed code.
   */
  static String addSchemaStringToEnumClass(String code) {
    Matcher enumMatcher = ENUM_CLASS_ANNOTATION_PATTERN.matcher(code);
    if (!enumMatcher.find()) {
      return code; // not a enum class
    }

    String packageName = null;
    String enumClassName;
    String enumSymbols;
    String doc = "auto-generated for avro compatibility";

    Matcher enumClassMatcher = ENUM_CLASS_DECL_PATTERN.matcher(code);
    if (!enumClassMatcher.find()) {
      throw new IllegalStateException("unable to find the enum declaration in " + code);
    }
    enumClassName = enumClassMatcher.group(1);
    enumSymbols = enumClassMatcher.group(2);

    Matcher packageMatcher = PACKAGE_PATTERN.matcher(code);
    if (packageMatcher.find()) { //optional
      packageName = packageMatcher.group(1);
    }

    Matcher commentMatcher = COMMENT_PATTERN.matcher(code);
    if (commentMatcher.find() && commentMatcher.start() < enumClassMatcher.start()) {
      //avro turns the doc property into a class-level comment
      String realDoc = commentMatcher.group(4);
      //remove anything that would otherwise require complicated escaping
      doc = realDoc.replaceAll("[\"'\\t\\n\\r]", " ") + " (auto-generated for avro compatibility)"; //retain the "auto-gen" bit
    }

    Map<String, String> templateParams = new HashMap<>();
    templateParams.put("name", enumClassName);
    templateParams.put("doc", doc);
    templateParams.put("namespace", packageName); //might be null
    templateParams.put("symbols", enumSymbols);
    StringBuilder sb = new StringBuilder();
    for (String enumSymbol : enumSymbols.split("\\s*,\\s*")) {
      sb.append("\\\\\"");
      sb.append(enumSymbol);
      sb.append("\\\\\",");
    }
    sb.deleteCharAt(sb.length() - 1);
    templateParams.put("symbol_string", sb.toString()); // drop the last comma

    String template = packageName == null ? ENUM_CLASS_NO_NAMESPACE_BODY_TEMPLATE : ENUM_CLASS_BODY_TEMPLATE;
    String body = populateTemplate(template, templateParams);

    return code.substring(0, enumMatcher.end(0)) + body;
  }

  /**
   * avro 1.4 generates bare-bone Fixed classes. modern avro expects Fixed classes (those that extend SpecificFixed):
   * <ul>
   *   <li>to have a public static final org.apache.avro.Schema SCHEMA$ field (at least avro 1.7)</li>
   *   <li>to have a public Schema getSchema() method (in avro 1.8) that returns the above SCHEMA$</li>
   *   <li>to have an implementation of the externalizable interface methods</li>
   * </ul>
   * some extra modern avro amenities that users may expect:
   * <ul>
   *   <li>a constructor that accepts a byte[] argument</li>
   * </ul>
   * this method introduces these into generated code for fixed classes
   * @param code generated code
   * @return if not a fixed class returns input. otherwise returns transformed code.
   */
  static String addMissingMethodsToFixedClass(String code) {
    Matcher fixedSizeMatcher = FIXED_SIZE_ANNOTATION_PATTERN.matcher(code);
    if (!fixedSizeMatcher.find()) {
      return code; //not a fixed record
    }

    int size;
    String packageName = null;
    String className;
    String doc = "auto-generated for avro compatibility";

    Matcher classMatcher = FIXED_CLASS_DECL_PATTERN.matcher(code);
    if (!classMatcher.find()) {
      throw new IllegalStateException("unable to find class declaration in " + code);
    }
    className = classMatcher.group(1);

    Matcher packageMatcher = PACKAGE_PATTERN.matcher(code);
    if (packageMatcher.find()) { //optional
      packageName = packageMatcher.group(1);
    }

    Matcher commentMatcher = COMMENT_PATTERN.matcher(code);
    if (commentMatcher.find() && commentMatcher.start() < classMatcher.start()) {
      //avro turns the doc property into a class-level comment
      String realDoc = commentMatcher.group(4);
      //remove anything that would otherwise require complicated escaping
      doc = realDoc.replaceAll("[\"'\\t\\n\\r]", "") + " (" + doc + ")"; //retain the "auto-gen" bit
    }

    try {
      size = Integer.parseInt(fixedSizeMatcher.group(1));
    } catch (NumberFormatException e) {
      throw new IllegalStateException("unable to parse size out of " + fixedSizeMatcher.group(0));
    }

    Map<String, String> templateParams = new HashMap<>();
    templateParams.put("name", className);
    templateParams.put("size", String.valueOf(size));
    templateParams.put("doc", doc);
    templateParams.put("namespace", packageName); //might be null
    String template = packageName == null ? FIXED_CLASS_NO_NAMESPACE_BODY_TEMPLATE : FIXED_CLASS_BODY_TEMPLATE;
    String body = populateTemplate(template, templateParams);

    return code.substring(0, classMatcher.end(0)) + body;
  }

  /**
   * java has a maximum size limit on string _LITERALS_, which generated schemas may go over,
   * producing uncompilable code (see see AVRO-1316).
   * this method replaces giant string literals in parse() invocations with a chain of
   * StringBuilder calls to build the giant String at runtime from smaller pieces.
   * @param code source code generated by avro 1.4
   * @return source code that wont have giant string literals in SCHEMA$
   */
  static String splitUpBigParseCalls(String code) {
    Matcher matcher = PARSE_INVOCATION_PATTERN.matcher(code); //group 1 would be the args to parse()
    if (!matcher.find()) {
      return code;
    }
    String stringLiteral = matcher.group(1);
    if (stringLiteral.length() < MAX_STRING_LITERAL_SIZE) {
      return code;
    }
    List<String> pieces = safeSplit(stringLiteral, MAX_STRING_LITERAL_SIZE);
    StringBuilder argBuilder = new StringBuilder(stringLiteral.length()); //at least
    argBuilder.append("new StringBuilder()");
    for (String piece : pieces) {
      argBuilder.append(".append(\"").append(piece).append("\")");
    }
    argBuilder.append(".toString()");
    return matcher.replaceFirst(Matcher.quoteReplacement("org.apache.avro.Schema.parse(" + argBuilder.toString() + ");"));
  }

  /**
   * splits a large java string literal into smaller pieces in a safe way.
   * by safe we mean avoids splitting anywhere near an escape sequence
   * @param javaStringLiteral large string literal
   * @return smaller string literals that can be joined to reform the argument
   */
  static List<String> safeSplit(String javaStringLiteral, int maxChunkSize) {
    String remainder = javaStringLiteral;
    List<String> results = new ArrayList<>(remainder.length() / maxChunkSize);
    while (remainder.length() > maxChunkSize) {
      int cutIndex = maxChunkSize;
      while (cutIndex > 0 && escapesNear(remainder, cutIndex)) {
        cutIndex--;
      }
      if (cutIndex <= 0) {
        //should never happen ...
        throw new IllegalStateException("unable to split " + javaStringLiteral);
      }
      String piece = remainder.substring(0, cutIndex);
      results.add(piece);
      remainder = remainder.substring(cutIndex);
    }
    if (!remainder.isEmpty()) {
      results.add(remainder);
    }
    return results;
  }

  /**
   * returns true is there's a string escape sequence starting anywhere
   * near a given index in a given string literal. since the longest escape
   * sequences in java are ~5-6 characters (unicode escapes) a safety margin
   * of 10 characters is used.
   * @param literal string literal to look for escape sequences in
   * @param index index around (before) which to look for escapes
   * @return true if any escape sequence found
   */
  static boolean escapesNear(String literal, int index) {
    //we start at index because we dont want the char at the start of the next fragment
    //to be an "interesting" character either
    for (int i = index; i > Math.max(0, index - 6); i--) {
      char c = literal.charAt(i);
      if (c == '\\' || c == '"' || c == '\'') {
        return true;
      }
    }
    return false;
  }

  static String loadTemplate(String templateName) {
    try (InputStream is = Avro14Factory.class.getClassLoader().getResourceAsStream(templateName)) {
      if (is == null) {
        throw new IllegalStateException("unable to find " + templateName);
      }
      try (InputStreamReader reader = new InputStreamReader(is, "UTF-8");
          BufferedReader bufferedReader = new BufferedReader(reader)) {
        StringWriter writer = new StringWriter();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          writer.append(line).append(System.lineSeparator());
        }
        writer.flush();
        return writer.toString();
      }
    } catch (Exception e) {
      throw new IllegalStateException("unable to load template " + templateName, e);
    }
  }

  static String populateTemplate(String template, Map<String, String> parameters) {
    //poor-man's regexp-based templating engine
    Matcher paramMatcher = TEMPLATE_PLACEHOLDER_PATTERN.matcher(template);
    StringBuffer output = new StringBuffer();
    while (paramMatcher.find()) {
      String paramName = paramMatcher.group(1);
      String paramValue = parameters.get(paramName);
      if (paramValue == null) {
        throw new IllegalStateException("parameters have no value for " + paramName);
      }
      paramMatcher.appendReplacement(output, paramValue);
    }
    paramMatcher.appendTail(output);
    return output.toString();
  }
}
