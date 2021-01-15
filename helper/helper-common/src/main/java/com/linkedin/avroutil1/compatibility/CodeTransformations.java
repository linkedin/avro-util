/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * utility class for transforming avro-generated java code
 */
public class CodeTransformations {

  private static final Pattern PACKAGE_PATTERN = Pattern.compile("package\\s+(.*);");
  private static final Pattern CLASS_PATTERN = Pattern.compile("public class (\\w+)");
  private static final Pattern COMMENT_PATTERN = Pattern.compile("(//([/\\s]*).*?\\s*$)|(/\\*+\\s*(.*?)\\s*\\*+/)", Pattern.MULTILINE | Pattern.DOTALL);
  private static final Pattern FIXED_SIZE_ANNOTATION_PATTERN = Pattern.compile("@org.apache.avro.specific.FixedSize\\((.*)\\)");
  private static final Pattern FIXED_CLASS_DECL_PATTERN = Pattern.compile("public class (\\w+) extends org.apache\\.avro\\.specific\\.SpecificFixed ");
  private static final Pattern ENUM_CLASS_DECL_PATTERN = Pattern.compile("public enum (\\w+)([^{]*)\\{");
  private static final Pattern PARSE_INVOCATION_START_PATTERN = Pattern.compile(
            "(" + Pattern.quote("org.apache.avro.Schema.parse(") + ")" + "|"
          + "(" + Pattern.quote("new org.apache.avro.Schema.Parser().parse(") + ")" + "|"
          + "(" + Pattern.quote(HelperConsts.HELPER_FQCN + ".parse(") + ")"
  );
  private static final Pattern PARSE_INVOCATION_END_PATTERN = Pattern.compile("\"\\);\\s*[\\r\\n]+");
  private static final Pattern PARSE_VARARG_PATTERN = Pattern.compile("[^\\\\]\",\""); // a non-escaped "," sequence
  private static final String  QUOTE = "\"";
  private static final String  ESCAPED_QUOTE = "\\\"";
  private static final String  BACKSLASH = "\\";
  private static final String  DOUBLE_BACKSLASH = "\\\\";
  private static final Pattern GET_CLASS_SCHEMA_PATTERN = Pattern.compile(Pattern.quote("public static org.apache.avro.Schema getClassSchema()"));
  private static final Pattern SCHEMA_DOLLAR_DECL_PATTERN = Pattern.compile(Pattern.quote("public static final org.apache.avro.Schema SCHEMA$"));
  private static final Pattern END_OF_SCHEMA_DOLLAR_DECL_PATTERN = Pattern.compile("}\"\\)(\\.toString\\(\\)\\))?;");
  private static final String  GET_CLASS_SCHEMA_METHOD = "public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }";
  private static final Pattern NEW_BUILDER_METHOD_PATTERN = Pattern.compile("public static ([\\w.]+) newBuilder\\(\\)");
  private static final Pattern END_BUILDER_CLASS_PATTERN = Pattern.compile("}\\s+}\\s+}");
  private static final Pattern FROM_BYTEBUFFER_METHOD_END_PATTERN = Pattern.compile("return DECODER.decode\\s*\\(\\s*b\\s*\\)\\s*;\\s*}");
  private static final Pattern IMPORT_CODECS_PATTERN = Pattern.compile("import org\\.apache\\.avro\\.message\\.([\\w.]+);");
  private static final Pattern AVROGENERATED_ANNOTATION_PATTERN = Pattern.compile(Pattern.quote("@org.apache.avro.specific.AvroGenerated"));
  private static final Pattern MODEL_DECL_PATTERN = Pattern.compile(Pattern.quote("private static SpecificData MODEL$ = new SpecificData();"));
  private static final Pattern GET_SPECIFICDATA_METHOD_PATTERN = Pattern.compile("public\\s*org\\.apache\\.avro\\.specific\\.SpecificData\\s*getSpecificData\\s*\\(\\s*\\)\\s*\\{\\s*return\\s*MODEL\\$\\s*;\\s*}");
  private static final Pattern WRITER_DOLLAR_DECL = Pattern.compile("WRITER\\$\\s*=\\s*([^;]+);");
  private static final String  WRITER_DOLLAR_DECL_REPLACEMENT = Matcher.quoteReplacement("WRITER$ = new org.apache.avro.specific.SpecificDatumWriter<>(SCHEMA$);");
  private static final Pattern READER_DOLLAR_DECL = Pattern.compile("READER\\$\\s*=\\s*([^;]+);");
  private static final String  READER_DOLLAR_DECL_REPLACEMENT = Matcher.quoteReplacement("READER$ = new org.apache.avro.specific.SpecificDatumReader<>(SCHEMA$);");
  private static final Pattern WRITE_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void writeExternal(java.io.ObjectOutput out)"));
  private static final String  WRITE_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void writeExternal(java.io.ObjectOutput out)");
  private static final Pattern READ_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void readExternal(java.io.ObjectInput in)"));
  private static final String  READ_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void readExternal(java.io.ObjectInput in)");
  private static final Pattern CREATE_ENCODER_INVOCATION_FULLY_QUALIFIED = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getEncoder(out)"));
  private static final Pattern CREATE_ENCODER_INVOCATION_SHORT = Pattern.compile(Pattern.quote("SpecificData.getEncoder(out)"));
  private static final String  CREATE_ENCODER_VIA_HELPER = Matcher.quoteReplacement(HelperConsts.HELPER_FQCN + ".newBinaryEncoder(out)");
  private static final Pattern CREATE_DECODER_INVOCATION_FULLY_QUALIFIED = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getDecoder(in)"));
  private static final Pattern CREATE_DECODER_INVOCATION_SHORT = Pattern.compile(Pattern.quote("SpecificData.getDecoder(in)"));
  private static final String  CREATE_DECODER_VIA_HELPER = Matcher.quoteReplacement(HelperConsts.HELPER_FQCN + ".newBinaryDecoder(in)");
  private static final Pattern HAS_CUSTOM_CODERS_SIGNATURE_SIGNATURE = Pattern.compile(Pattern.quote("@Override protected boolean hasCustomCoders"));
  private static final Pattern END_CUSTOM_DECODE_PATTERN = Pattern.compile("}\\s+}\\s+}\\s+}\\s*[\\r\\n]+");

  private static final String FIXED_CLASS_BODY_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/SpecificFixedBody.template");
  private static final String FIXED_CLASS_NO_NAMESPACE_BODY_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/SpecificFixedBodyNoNamespace.template");
  private static final String ENUM_CLASS_BODY_UNIVERSAL_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassUniversal.template");
  private static final String ENUM_CLASS_NO_NAMESPACE_BODY_UNIVERSAL_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassNoNamespaceUniversal.template");
  private static final String ENUM_CLASS_BODY_PRE19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassPre19.template");
  private static final String ENUM_CLASS_NO_NAMESPACE_BODY_PRE19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassNoNamespacePre19.template");
  private static final String ENUM_CLASS_BODY_POST19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClass19.template");
  private static final String ENUM_CLASS_NO_NAMESPACE_BODY_POST19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassNoNamespace19.template");

  private static final int MAX_STRING_LITERAL_SIZE = 65000; //just under 64k

  /**
   * applies all transformations to a java class generated by avro
   * @param code raw java code for a class generated by avro
   * @param generatedBy major version of avro that generated the input code
   * @param minSupportedVersion minimum major avro version under which the result should "work"
   * @param maxSupportedVersion maximum major avro version under which the result should "work"
   * @return fixed-up code
   */
  public static String applyAll(String code, AvroVersion generatedBy, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
    String fixed = code;

    //general fix-ups that are considered safe regardless of min or max avro version
    fixed = CodeTransformations.transformFixedClass(fixed, minSupportedVersion, maxSupportedVersion);
    fixed = CodeTransformations.transformEnumClass(fixed, minSupportedVersion, maxSupportedVersion);
    fixed = CodeTransformations.transformParseCalls(fixed, generatedBy, minSupportedVersion, maxSupportedVersion);
    fixed = CodeTransformations.addGetClassSchemaMethod(fixed, generatedBy, minSupportedVersion, maxSupportedVersion);

    //1.6+ features
    if (minSupportedVersion.earlierThan(AvroVersion.AVRO_1_6)) {
      //optionally strip out builders
      fixed = CodeTransformations.removeBuilderSupport(fixed, minSupportedVersion, maxSupportedVersion);
    }

    //1.7+ features
    //this is considered harmless enough we can keep doing it?
    fixed = CodeTransformations.removeAvroGeneratedAnnotation(fixed, minSupportedVersion, maxSupportedVersion);

    //1.8+ features
    fixed = CodeTransformations.removeBinaryMessageCodecSupport(fixed, minSupportedVersion, maxSupportedVersion);
    fixed = CodeTransformations.transformExternalizableSupport(fixed, minSupportedVersion, maxSupportedVersion);

    //1.9+ features
    fixed = CodeTransformations.transformCustomCodersSupport(fixed, minSupportedVersion, maxSupportedVersion);

    return fixed;
  }

  /**
   * this method transforms the code for a generated class for an avro "fixed" type to make it compatible with a
   * range of avro versions.<br>
   *
   * modern(ish) versions of avro expect a fixed class (one that extends {@link org.apache.avro.specific.SpecificFixed})
   * to have the following amenities:
   * <ul>
   *   <li>public static final org.apache.avro.Schema SCHEMA$ field (avro 1.5+)</li>
   *   <li>a public Schema getSchema() method (in avro 1.8) that returns the above SCHEMA$</li>
   *   <li>an implementation of the {@link java.io.Externalizable} interface methods (avro 1.8+)</li>
   * </ul>
   * some extra modern avro amenities that avro itself doesnt require but users may expect:
   * <ul>
   *   <li>an extra constructor that accepts a byte[] argument (avro 1.6+)</li>
   *   <li>a static getClassSchema() method that returns SCHEMA$ (avro 1.7+)</li>
   *   <li>an AvroGenerated annotation (avro 1.7+) - note this annotation only exists in avro 1.7+</li>
   *   <li>an explicit serialVersionUID (avro 1.8+)</li>
   * </ul>
   * this method introduces these into generated code for fixed classes (when possible)
   * @param code generated code
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return if not a fixed class returns input. otherwise returns transformed code.
   */
  public static String transformFixedClass(String code, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
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
    templateParams.put("helper", HelperConsts.HELPER_FQCN);
    String template = packageName == null ? FIXED_CLASS_NO_NAMESPACE_BODY_TEMPLATE : FIXED_CLASS_BODY_TEMPLATE;
    String body = TemplateUtil.populateTemplate(template, templateParams);

    return code.substring(0, classMatcher.end(0)) + body;
  }

/**
 * this method transforms the code for a generated class for an avro "fixed" type to make it compatible with a
 * range of avro versions.<br>
 *
 * modern(ish) versions of avro expect an enum class to have the following amenities:
 * <ul>
 *   <li>public static final org.apache.avro.Schema SCHEMA$ field (avro 1.5+)</li>
 *   <li>a public Schema getSchema() method that returns the above SCHEMA$ (avro 1.9+)</li>
 *   <li>to implement {@link org.apache.avro.generic.GenericEnumSymbol} (avro 1.9+)</li>
 * </ul>
 * some extra modern avro amenities that avro itself doesnt require but users may expect:
 * <ul>
 *   <li>a static getClassSchema() method that returns SCHEMA$ (avro 1.7+)</li>
 *   <li>an AvroGenerated annotation (avro 1.7+) - note this annotation only exists in avro 1.7+</li>
 * </ul>
 * this method introduces these into generated code for fixed classes (when possible)
 * @param code generated code
 * @param minSupportedVersion lowest avro version under which the generated code should work
 * @param maxSupportedVersion highest avro version under which the generated code should work
 * @return if not an enum class returns input. otherwise returns transformed code.
 */
  public static String transformEnumClass(String code, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
    Matcher enumMatcher = ENUM_CLASS_DECL_PATTERN.matcher(code);
    if (!enumMatcher.find()) {
      return code; // not a enum class
    }
    String enumClassName = enumMatcher.group(1);

    String packageName = null;
    String enumSymbols; //will hold the csv of symbols, for the java code
    String enumQuotedSymbols; //will hold the csv of quoted literals, for embedded schema
    String doc = "auto-generated for avro compatibility";

    int endOfSymbolsLine = code.indexOf(";", enumMatcher.end());
    if (endOfSymbolsLine < 0) {
      //old avro has nothing following the symbols, so doesnt bother terminating the line
      endOfSymbolsLine = code.indexOf("}", enumMatcher.end());
    }
    String[] symbolParts = code.substring(enumMatcher.end(), endOfSymbolsLine).split("\\s*,\\s*");

    StringJoiner literalsCsv = new StringJoiner(", ");
    StringJoiner quotedLiteralsCsv = new StringJoiner(", ");
    for (String part : symbolParts) {
      String cleanUp = part.trim();
      if (!cleanUp.isEmpty()) {
        literalsCsv.add(cleanUp);
        quotedLiteralsCsv.add("\\\\\"" + cleanUp + "\\\\\"");
      }
    }

    enumSymbols = literalsCsv.toString();
    enumQuotedSymbols = quotedLiteralsCsv.toString();

    int packageEnd = 0;
    Matcher packageMatcher = PACKAGE_PATTERN.matcher(code);
    if (packageMatcher.find()) { //optional
      packageName = packageMatcher.group(1);
      packageEnd = packageMatcher.end();
    }

    Matcher commentMatcher = COMMENT_PATTERN.matcher(code);
    if (commentMatcher.find(packageEnd) && commentMatcher.start() < enumMatcher.start()) {
      //avro turns the doc property into a class-level comment following the package declaration (if any)
      String realDoc = commentMatcher.group(4);
      //remove anything that would otherwise require complicated escaping
      doc = realDoc.replaceAll("[\"'\\t\\n\\r]", " ") + " (auto-generated for avro compatibility)"; //retain the "auto-gen" bit
    }

    Map<String, String> templateParams = new HashMap<>();
    templateParams.put("name", enumClassName);
    templateParams.put("doc", doc);
    templateParams.put("namespace", packageName); //might be null
    templateParams.put("symbols", enumSymbols);
    templateParams.put("symbol_string", enumQuotedSymbols);
    templateParams.put("helper", HelperConsts.HELPER_FQCN);

    //trying to compile "X implements GenericEnumSymbol" under avro 1.9+ results in
    //'java.lang.Comparable' cannot be inherited with different type arguments: 'X' and 'null'
    //so if we're targeting a broad avro range that spans 1.9 we need to strip out the "implements GenericEnumSymbol" completely

    String template;
    if (maxSupportedVersion.earlierThan(AvroVersion.AVRO_1_9)) {
      template = packageName == null ? ENUM_CLASS_NO_NAMESPACE_BODY_PRE19_TEMPLATE : ENUM_CLASS_BODY_PRE19_TEMPLATE;
    } else {
      if (minSupportedVersion.laterThan(AvroVersion.AVRO_1_8)) {
        template = packageName == null ? ENUM_CLASS_NO_NAMESPACE_BODY_POST19_TEMPLATE : ENUM_CLASS_BODY_POST19_TEMPLATE;
      } else {
        template = packageName == null ? ENUM_CLASS_NO_NAMESPACE_BODY_UNIVERSAL_TEMPLATE : ENUM_CLASS_BODY_UNIVERSAL_TEMPLATE;
      }
    }

    String body = TemplateUtil.populateTemplate(template, templateParams);

    //this picks everything before the class declaration
    String classPrefix = code.substring(0, enumMatcher.start(0));

    return classPrefix + body;
  }

  /**
   * java has a maximum size limit on string _LITERALS_, which generated schemas may go over,
   * producing uncompilable code (see see AVRO-1316). this was only fixed in avro 1.7.5+
   *
   * also - avro 1.6+ issues "new org.apache.avro.Schema.Parser().parse(...)" calls which will not compile
   * under {@literal avro < 1.6}
   *
   * in addition, {@literal avro < 1.6} fails to properly escape control characters in the schema string (like newlines
   * in doc properties) which will result in a json parse error when trying to instantiate the
   * generated java class (because at that point it will fail to parse the avsc in SCHEMA$)
   *
   * this method does 3 things:
   * <ul>
   *   <li>replaces all parse calls with Helper.parse() calls</li>
   *   <li>replaces giant literals inside the parse() calls with a StringBuilder</li>
   *   <li>properly escapes any control characters in string literals inside the avsc</li>
   * </ul>
   * @param code avro generated source code which may have giant string literals in parse() calls
   * @param generatedWith version of avro that generated the original code
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return source code that wont have giant string literals in SCHEMA$
   */
  public static String transformParseCalls(
      String code,
      AvroVersion generatedWith,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion
  ) {
    Matcher startMatcher = PARSE_INVOCATION_START_PATTERN.matcher(code); //group 1 would be the args to parse()
    if (!startMatcher.find()) {
      return code;
    }
    Matcher endMatcher = PARSE_INVOCATION_END_PATTERN.matcher(code);
    if (!endMatcher.find(startMatcher.end())) {
      throw new IllegalStateException("found the start of a parse expression but not the ending in " + code);
    }

    //does not include the enclosing double quotes
    String stringLiteral = code.substring(startMatcher.end() + 1, endMatcher.start());
    boolean largeString = stringLiteral.length() >= MAX_STRING_LITERAL_SIZE;
    //either we've already been here, or modern avro was used that already emits vararg
    boolean ourVararg = stringLiteral.contains("new StringBuilder().append(");
    boolean avroVararg = PARSE_VARARG_PATTERN.matcher(stringLiteral).find();
    boolean alreadyVararg = ourVararg || avroVararg;

    //1st lets find any escape characters inside string literal(s) that may need escaping
    //(if avro did the vararg avro also did escaping. likewise if we already did)
    String escapedLiterals;
    if (alreadyVararg || generatedWith.laterThan(AvroVersion.AVRO_1_5)) {
      escapedLiterals = stringLiteral;
    } else {
      escapedLiterals = escapeJavaLiteral(stringLiteral);
    }

    String argToParseCall;
    if (largeString && !alreadyVararg) {
      List<String> pieces = safeSplit(escapedLiterals, MAX_STRING_LITERAL_SIZE);
      StringBuilder argBuilder = new StringBuilder(escapedLiterals.length()); //at least
      argBuilder.append("new StringBuilder()");
      for (String piece : pieces) {
        argBuilder.append(".append(\"").append(piece).append("\")");
      }
      argBuilder.append(".toString()");
      argToParseCall = argBuilder.toString();
    } else {
      argToParseCall = "\"" + escapedLiterals + "\"";
    }

    String prefix = code.substring(0, startMatcher.start());
    String newParseCall = HelperConsts.HELPER_FQCN + ".parse(" + argToParseCall + ");";
    String restOfCode = code.substring(endMatcher.start() + 3);
    return prefix + newParseCall + restOfCode;
  }

  /**
   * escapes any control characters inside string literals that are part of an argument json
   * PACKAGE PRIVATE FOR TESTING
   * @param str a piece of json, as a java string literal
   * @return the given piece of json, safe for use as a java string literal
   */
  static String escapeJavaLiteral(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }

    int quotedStrStart; //start of current quoted string (index of the starting quote character)
    int quotedStrEnd;   //end of current quoted string (index of the end quote character)
    int lastEnd = 0;    //end of previous quoted string (index of the end quote character)
    StringBuilder result = new StringBuilder(str.length());

    //this code searches for json string literals inside a java string literal
    //these would look like "{\"prop\":\"value\"}". we do this by finding pairs of \" (escaped quotes)
    //and then making sure whats between them (the json string literals) is properly escaped

    quotedStrStart = str.indexOf(ESCAPED_QUOTE, lastEnd);
    while (quotedStrStart >= 0) {
      quotedStrEnd = quotedStrStart;
      while (true) {
        quotedStrEnd = str.indexOf(ESCAPED_QUOTE, quotedStrEnd + 2);
        if (quotedStrEnd < 0) {
          throw new IllegalArgumentException(
              "unterminated string literal starting at offset " + quotedStrStart + " in " + str);
        }
        char precedingChar = str.charAt(quotedStrEnd - 1); //character before the closing quote
        if (precedingChar != '\\') {
          break;
        }
      }

      String quotedStr = str.substring(quotedStrStart + 2, quotedStrEnd); //without enclosing quotes

      //we assume we start with a properly escaped json inside, so we double all backslashes, then take one out
      //if it was escaping a quote :-)
      String escaped = quotedStr.replace(BACKSLASH, DOUBLE_BACKSLASH).replace(ESCAPED_QUOTE, QUOTE);

      //copy from end of last string to start of this one (included our start quote)
      result.append(str, lastEnd, quotedStrStart + 2);
      //append this string, escaped
      result.append(escaped);

      //move on to the next quoted string (if any)
      lastEnd = quotedStrEnd;
      quotedStrStart = str.indexOf(ESCAPED_QUOTE, lastEnd + 1);
    }

    //append the tail (there's always at least the last end quote)
    result.append(str, lastEnd, str.length());

    return result.toString();
  }

  /**
   * avro 1.7+ adds a static getClassSchema() method that returns SCHEMA$
   * @param code avro generated source code which may lack getClassSchema()
   * @param generatedWith version of avro that generated the original code
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return source code that contains getClassSchema()
   */
  public static String addGetClassSchemaMethod(
      String code,
      AvroVersion generatedWith,
      AvroVersion minSupportedVersion,
      AvroVersion maxSupportedVersion
  ) {
    Matcher classMatcher = GET_CLASS_SCHEMA_PATTERN.matcher(code);
    if (classMatcher.find()) {
      //this code already has the method
      return code;
    }
    int schema$EndPosition = findEndOfSchemaDeclaration(code);
    return code.substring(0, schema$EndPosition) + "\n  " + GET_CLASS_SCHEMA_METHOD + "\n" + code.substring(schema$EndPosition);
  }

  /**
   * find the end of the SCHEMA$ declaration, for either simple declarations or those where the schema is huge and
   * we had to use a StringBuilder
   * @param code avro class code
   * @return location of the ending of the SCHEMA$ declaration i the given code
   */
  static int findEndOfSchemaDeclaration(String code) {
    Matcher schema$Matcher = SCHEMA_DOLLAR_DECL_PATTERN.matcher(code);
    if (!schema$Matcher.find()) {
      throw new IllegalStateException("unable to locate SCHEMA$ in " + code);
    }
    int schema$Position = schema$Matcher.end();
    //locate the end of the SCHEMA$ declaration (allow to StringBuilder().toString() code if the schema
    //was huge and we split is in transformParseCalls())
    Matcher schema$EndMatcher = END_OF_SCHEMA_DOLLAR_DECL_PATTERN.matcher(code);
    if (!schema$EndMatcher.find(schema$Position)) {
      throw new IllegalStateException("unable to locate SCHEMA$ ending in " + code);
    }
    return schema$EndMatcher.end();
  }

  /**
   * avro 1.6+ adds builder support to all generated record classes. the problem is that these builders all extend
   * org.apache.avro.specific.SpecificRecordBuilderBase, which doeswnt exist before avro 1.6
   * this method strips out the builder support
   * @param code avro generated source code which may have builder support
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return code without the builder support
   */
  public static String removeBuilderSupport(String code, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
    Matcher classMatcher = CLASS_PATTERN.matcher(code);
    if (!classMatcher.find()) {
      //this isnt a record class (maybe its an enum). nothing to do
      return code;
    }
    String className = classMatcher.group(1);

    Matcher builderMethodMatcher = NEW_BUILDER_METHOD_PATTERN.matcher(code);
    if (!builderMethodMatcher.find()) {
      //no builder (generated by avro <1.6?)
      return code;
    }

    //we look for the start of the javadoc on the 1st newBuilder method
    int methodStart = code.lastIndexOf("/**", builderMethodMatcher.start());

    //now find the last method defined on the builder class - public <SimpleName> build()
    Pattern buildMethodPattern = Pattern.compile("public " + Pattern.quote(className) + " build\\(\\)");
    Matcher buildMethodMatcher = buildMethodPattern.matcher(code);
    if (!buildMethodMatcher.find(builderMethodMatcher.end())) {
      throw new IllegalStateException("cant locate builder support block in " + code);
    }

    //find the end of the inner builder class
    Matcher endBuilderMatcher = END_BUILDER_CLASS_PATTERN.matcher(code);
    if (!endBuilderMatcher.find(builderMethodMatcher.end())) {
      throw new IllegalStateException("cant locate builder support block in " + code);
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    String codeWithoutBuilder = code.substring(0, methodStart) + code.substring(endBuilderMatcher.end());
    return codeWithoutBuilder;
  }

  /**
   * avro 1.8+ introduced classes org.apache.avro.message.BinaryMessageEncoder and
   * org.apache.avro.message.BinaryMessageDecoder and methods that use them to support
   * encoding and decoding such generates classes (toByteBuffer(), fromByteBuffer() and
   * static getDecoder()).
   * since these classes dont exist before avro 1.8 we strip them out
   * @param code avro generated code that might have BinaryMessageEncoder/Decoder usage
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return code without Encoder/Decoder usage
   */
  public static String removeBinaryMessageCodecSupport(String code, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
    int encoderStart = code.indexOf("private static final BinaryMessageEncoder");
    if (encoderStart < 0) {
      return code; //no encoder/decoder code
    }

    //in both 1.8 and 1.9, the codec-related block ends with the end of "fromByteBuffer"
    Matcher endOfFromBBMethodMatcher = FROM_BYTEBUFFER_METHOD_END_PATTERN.matcher(code);
    if (!endOfFromBBMethodMatcher.find()) {
      throw new IllegalStateException("unable to find Encoder/Decoder support in " + code);
    }

    String codeWithoutCodecs = code.substring(0, encoderStart) + code.substring(endOfFromBBMethodMatcher.end());

    //last thing to remove are the imports for these codec classes at they would cause code to not compile under avro < 1.8
    @SuppressWarnings("UnnecessaryLocalVariable")
    String codeWithoutImports = IMPORT_CODECS_PATTERN.matcher(codeWithoutCodecs).replaceAll("");

    return codeWithoutImports;
  }

  /**
   * the org.apache.avro.specific.AvroGenerated annotation was introduced in avro 1.7+
   * if we intend to compile generated code under any older version we need to strip it out.
   * @param code avro generated code that might have the AvroGenerated annotation
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return code without the annotation
   */
  public static String removeAvroGeneratedAnnotation(String code, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
    return AVROGENERATED_ANNOTATION_PATTERN.matcher(code).replaceAll("");
  }

  /**
   * {@link org.apache.avro.specific.SpecificRecordBase} implements {@link java.io.Externalizable} starting with avro 1.8+
   * trying to compile code generated by 1.8+ under older avro will result in compilation errors as the {@link Override}
   * annotation on the Externalizable methods will not actually be overriding anything.
   *
   * in addition, this support is driven by a few new fields:
   * <ul>
   *   <li>
   *     private static SpecificData MODEL$ = new SpecificData(); the SpecificData constructor was only
   *     made public in avro 1.7+. to support older avro we need to replace that with SpecificData.get()
   *   </li>
   *   <li>
   *     private static final org.apache.avro.io.DatumWriter WRITER$, optionally initialized using MODEL$
   *     (if MODEL$ exists) or directly (for classes for fixed types)
   *   </li>
   *   <li>
   *     private static final org.apache.avro.io.DatumReader READER$ - again optionally initialized via MODEL$
   *   </li>
   * </ul>
   *
   * and finally the methods:
   * <ul>
   *   <li>
   *     public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; } - this was
   *     introduced in avro 1.9+
   *   </li>
   *   <li>
   *     writeExternal()/readExternal() methods have @Override. this only compiles under avro 1.8+
   *     as before that the generated code does not implement Externalizable
   *   </li>
   *   <li>
   *     SpecificData.getEncoder()/getDecoder() methods used as part of writeExternal()/readExternal() only exist
   *     in avro 1.8+
   *   </li>
   * </ul>
   *
   *
   * @param code avro generated code that may have externalizable support
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return code where the externalizable support still exists but is compatible with earlier avro at runtime
   */
  public static String transformExternalizableSupport(String code, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
    //strip out MODEL$ completely
    String codeWithoutModel = MODEL_DECL_PATTERN.matcher(code).replaceAll("");
    //then strip out the getSpecificData() method that returns MODEL$ under avro 1.9+
    String codeWithoutGetSpecificData = GET_SPECIFICDATA_METHOD_PATTERN.matcher(codeWithoutModel).replaceAll("");

    //this is actually a nop for fixed classes
    String writerFixed = WRITER_DOLLAR_DECL.matcher(codeWithoutGetSpecificData).replaceAll(
        WRITER_DOLLAR_DECL_REPLACEMENT);
    //this is actually a nop for fixed classes
    String readerFixed = READER_DOLLAR_DECL.matcher(writerFixed).replaceAll(READER_DOLLAR_DECL_REPLACEMENT);

    //strip out the @Override annotations from readExternal()/writeExternal()
    String withoutAnnotations = WRITE_EXTERNAL_SIGNATURE.matcher(readerFixed).replaceAll(WRITE_EXTERNAL_WITHOUT_OVERRIDE);
    withoutAnnotations = READ_EXTERNAL_SIGNATURE.matcher(withoutAnnotations).replaceAll(READ_EXTERNAL_WITHOUT_OVERRIDE);

    //replace SpecificData.getEncoder()/getDecoder() with the helper
    String withHelperCall = CREATE_ENCODER_INVOCATION_FULLY_QUALIFIED.matcher(withoutAnnotations).replaceAll(CREATE_ENCODER_VIA_HELPER);
    withHelperCall = CREATE_ENCODER_INVOCATION_SHORT.matcher(withHelperCall).replaceAll(CREATE_ENCODER_VIA_HELPER);
    withHelperCall = CREATE_DECODER_INVOCATION_FULLY_QUALIFIED.matcher(withHelperCall).replaceAll(CREATE_DECODER_VIA_HELPER);
    withHelperCall = CREATE_DECODER_INVOCATION_SHORT.matcher(withHelperCall).replaceAll(CREATE_DECODER_VIA_HELPER);

    return withHelperCall;
  }

  /**
   * avro 1.9 introduced 3 new methods to {@link org.apache.avro.specific.SpecificRecordBase}:
   * <ul>
   *   <li>protected boolean hasCustomCoders()</li>
   *   <li>public void customEncode</li>
   *   <li>public void customDecode</li>
   * </ul>
   *
   * the implementation of customDecode() relies on ResolvingDecoder.readFieldOrderIfDiff()
   * which only exists in avro 1.9, so under older avro we strip it all out
   *
   * @param code avro generated code that may have custom encode/decode support
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @return code where the custom codec support has been removed
   */
  public static String transformCustomCodersSupport(String code, AvroVersion minSupportedVersion, AvroVersion maxSupportedVersion) {
    Matcher startMatcher = HAS_CUSTOM_CODERS_SIGNATURE_SIGNATURE.matcher(code);
    if (!startMatcher.find()) {
      return code; //no codec support in this code
    }
    Matcher endMatcher = END_CUSTOM_DECODE_PATTERN.matcher(code);
    if (!endMatcher.find(startMatcher.end())) {
      throw new IllegalStateException("unable to find custom Encoder/Decoder support in " + code);
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    String codeWithout = code.substring(0, startMatcher.start()) + "\n" + code.substring(endMatcher.end());
    return codeWithout;
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
}
