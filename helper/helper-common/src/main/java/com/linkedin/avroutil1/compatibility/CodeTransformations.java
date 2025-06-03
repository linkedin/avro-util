/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.apache.commons.text.StringEscapeUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final Pattern GET_CLASS_SCHEMA_PATTERN = Pattern.compile(Pattern.quote("public static org.apache.avro.Schema getClassSchema()"));
  private static final Pattern SCHEMA_DOLLAR_DECL_PATTERN = Pattern.compile(Pattern.quote("public static final org.apache.avro.Schema SCHEMA$"));
  private static final Pattern END_OF_SCHEMA_DOLLAR_DECL_PATTERN = Pattern.compile("}\"\\)(\\.toString\\(\\)\\))?;");
  private static final String  GET_CLASS_SCHEMA_METHOD = "public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }";
  private static final Pattern NEW_BUILDER_METHOD_PATTERN = Pattern.compile("public static ([\\w.]+) newBuilder\\(\\)");
  private static final Pattern END_BUILDER_CLASS_PATTERN = Pattern.compile("}\\s+}\\s+}");
  private static final Pattern FROM_BYTEBUFFER_METHOD_END_PATTERN = Pattern.compile("return DECODER.decode\\s*\\(\\s*b\\s*\\)\\s*;\\s*}");
  private static final Pattern IMPORT_CODECS_PATTERN = Pattern.compile("import org\\.apache\\.avro\\.message\\.([\\w.]+);");
  private static final Pattern AVROGENERATED_ANNOTATION_PATTERN = Pattern.compile(Pattern.quote("@org.apache.avro.specific.AvroGenerated"));
  //avro < 1.11 has "private static SpecificData MODEL$", 1.11+ has "private static final SpecificData MODEL$"
  private static final Pattern MODEL_DECL_PATTERN = Pattern.compile("private static (final )?SpecificData MODEL\\$ = new SpecificData\\(\\);");
  private static final Pattern MODEL_ADD_TYPE_CONVERSION_PATTERN = Pattern.compile("MODEL\\$\\.addLogicalTypeConversion\\(.*\\);");
  private static final String  MODEL_DECL_REPLACEMENT = "private static final org.apache.avro.specific.SpecificData MODEL$ = SpecificData.get();";
  private static final String  IMPORT_SPECIFICDATA = "import org.apache.avro.specific.SpecificData;";
  private static final Pattern GET_SPECIFICDATA_METHOD_PATTERN = Pattern.compile("(@Override\n\\s*)public\\s*org\\.apache\\.avro\\.specific\\.SpecificData\\s*getSpecificData\\s*\\(\\s*\\)\\s*\\{\\s*return\\s*MODEL\\$\\s*;\\s*}");
  private static final Pattern WRITER_DOLLAR_DECL = Pattern.compile("WRITER\\$\\s*=\\s*([^;]+);");
  private static final String  WRITER_DOLLAR_DECL_REPLACEMENT = Matcher.quoteReplacement("WRITER$ = " + HelperConsts.HELPER_SIMPLE_NAME + ".newSpecificDatumWriter(SCHEMA$, MODEL$);");
  private static final Pattern READER_DOLLAR_DECL = Pattern.compile("READER\\$\\s*=\\s*([^;]+);");
  private static final String  READER_DOLLAR_DECL_REPLACEMENT = Matcher.quoteReplacement("READER$ = " + HelperConsts.HELPER_SIMPLE_NAME + ".newSpecificDatumReader(SCHEMA$, SCHEMA$, MODEL$);");
  private static final Pattern WRITE_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void writeExternal(java.io.ObjectOutput out)"));
  private static final String  WRITE_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void writeExternal(java.io.ObjectOutput out)");
  private static final Pattern READ_EXTERNAL_SIGNATURE = Pattern.compile(Pattern.quote("@Override public void readExternal(java.io.ObjectInput in)"));
  private static final String  READ_EXTERNAL_WITHOUT_OVERRIDE = Matcher.quoteReplacement("public void readExternal(java.io.ObjectInput in)");
  private static final Pattern CREATE_ENCODER_INVOCATION_FULLY_QUALIFIED = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getEncoder(out)"));
  private static final Pattern CREATE_ENCODER_INVOCATION_SHORT = Pattern.compile(Pattern.quote("SpecificData.getEncoder(out)"));
  private static final String  CREATE_ENCODER_VIA_HELPER = Matcher.quoteReplacement(HelperConsts.HELPER_SIMPLE_NAME + ".newBinaryEncoder(out)");
  private static final Pattern CREATE_DECODER_INVOCATION_FULLY_QUALIFIED = Pattern.compile(Pattern.quote("org.apache.avro.specific.SpecificData.getDecoder(in)"));
  private static final Pattern CREATE_DECODER_INVOCATION_SHORT = Pattern.compile(Pattern.quote("SpecificData.getDecoder(in)"));
  private static final String  CREATE_DECODER_VIA_HELPER = Matcher.quoteReplacement(HelperConsts.HELPER_SIMPLE_NAME + ".newBinaryDecoder(in)");
  private static final Pattern HAS_CUSTOM_CODERS_SIGNATURE_SIGNATURE = Pattern.compile(Pattern.quote("@Override protected boolean hasCustomCoders"));
  private static final Pattern END_CUSTOM_DECODE_PATTERN = Pattern.compile("}\\s+}\\s+}\\s+}\\s*[\\r\\n]+");
  private static final Pattern IMPORT_PATTERN = Pattern.compile("import\\s+(.*);");
  private static final Pattern ENUM_PATTERN = Pattern.compile("public enum (\\w+)");
  private static final Pattern ANNOTATION_PATTERN = Pattern.compile("\\s*@.*");
  private static final Pattern CATCH_MISSING_FIELD_AND_REGULAR_EXCEPTION_PATTERN = Pattern.compile("catch \\(org\\.apache\\.avro\\.AvroMissingFieldException e\\) \\{\\s*"
      + "throw e;\\s*"
      + "} catch \\((java\\.lang\\.)?Exception e\\) \\{\\s*"
      + "throw new org\\.apache\\.avro\\.AvroRuntimeException\\(e\\);\\s*"
      + "}");
  private static final Pattern CATCH_MISSING_FIELD_EXCEPTION_START_PATTERN = Pattern.compile("catch \\(org\\.apache\\.avro\\.AvroMissingFieldException e\\) \\{\\s*");
  private static final Pattern CATCH_MISSING_FIELD_EXCEPTION_END_PATTERN = Pattern.compile("throw e;\\s*}");
  private static final String  COMPATIBLE_CATCH_REPLACEMENT = Matcher.quoteReplacement("catch (java.lang.Exception e) {\n" +
          "        throw e instanceof org.apache.avro.AvroRuntimeException ? (org.apache.avro.AvroRuntimeException) e : new org.apache.avro.AvroRuntimeException(e);\n" +
          "      }");
  private static final String  IMPORT_HELPER = "import " + HelperConsts.HELPER_FQCN + ";";
  private static final Pattern CATCH_UNQUALIFIED_EXCEPTION_PATTERN = Pattern.compile("catch\\s*\\(\\s*Exception\\s+");
  private static final String  CATCH_QUALIFIED_EXCEPTION = Matcher.quoteReplacement("catch (java.lang.Exception ");
  private static final Pattern BUILDER_CLASS_PATTERN = Pattern.compile("^\\s*implements org.apache.avro.data.RecordBuilder<\\w+> \\{$", Pattern.MULTILINE);
  private static final Pattern BUILDER_SUPER_PATTERN = Pattern.compile("^\\s*super\\([\\w.]*SCHEMA\\$(?:, [\\w.]*MODEL\\$)?\\);$", Pattern.MULTILINE);
  private static final String BUILDER_INSTANCE_NAME = "BUILDER_INSTANCE$";
  private static final String BUILDER_SUPER_REPLACEMENT = Matcher.quoteReplacement("this(" + BUILDER_INSTANCE_NAME + ");");

  private static final String FIXED_CLASS_BODY_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/SpecificFixedBody.template");
  private static final String FIXED_CLASS_NO_NAMESPACE_BODY_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/SpecificFixedBodyNoNamespace.template");
  private static final String ENUM_CLASS_BODY_UNIVERSAL_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassUniversal.template");
  private static final String ENUM_CLASS_NO_NAMESPACE_BODY_UNIVERSAL_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassNoNamespaceUniversal.template");
  private static final String ENUM_CLASS_BODY_PRE19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassPre19.template");
  private static final String ENUM_CLASS_NO_NAMESPACE_BODY_PRE19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassNoNamespacePre19.template");
  private static final String ENUM_CLASS_BODY_POST19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClass19.template");
  private static final String ENUM_CLASS_NO_NAMESPACE_BODY_POST19_TEMPLATE = TemplateUtil.loadTemplate("avroutil1/templates/EnumClassNoNamespace19.template");

  private static final int MAX_STRING_LITERAL_SIZE = 65000; //just under 64k

  private CodeTransformations() {

  }

  /**
   * applies all transformations to a java class generated by avro
   * @param code raw java code for a class generated by avro
   * @param generatedBy major version of avro that generated the input code
   * @param minSupportedVersion minimum major avro version under which the result should "work"
   * @param maxSupportedVersion maximum major avro version under which the result should "work"
   * @param alternativeAvsc alternative avsc to use in SCHEMA$
   * @return fixed-up code
   */
  public static String applyAll(
          String code,
          AvroVersion generatedBy,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion,
          String alternativeAvsc
  ) {
    String fixed = code;
    Set<String> importStatements = new HashSet<>(1);

    //general fix-ups that are considered safe regardless of min or max avro version
    fixed = CodeTransformations.transformFixedClass(fixed, minSupportedVersion, maxSupportedVersion);
    fixed = CodeTransformations.transformEnumClass(fixed, minSupportedVersion, maxSupportedVersion);
    fixed = CodeTransformations.transformParseCalls(fixed, generatedBy, minSupportedVersion, maxSupportedVersion, alternativeAvsc, importStatements);
    fixed = CodeTransformations.addGetClassSchemaMethod(fixed, generatedBy, minSupportedVersion, maxSupportedVersion);

    // Allow int fields to be set using longs, and vice versa
    fixed = CodeTransformations.enhanceNumericPutMethod(fixed);
    fixed = CodeTransformations.addOverloadedNumericSetterMethods(fixed);
    fixed = CodeTransformations.addOverloadedNumericConstructor(fixed);

    //1.6+ features
    if (minSupportedVersion.earlierThan(AvroVersion.AVRO_1_6)) {
      //optionally strip out builders
      fixed = CodeTransformations.removeBuilderSupport(fixed, minSupportedVersion, maxSupportedVersion);
    } else {
      //if we keep the builders we might have to strip out some references in generated code to
      //classes that only exist in more "modern" avro
      fixed = CodeTransformations.removeReferencesToNewClassesFromBuilders(fixed, generatedBy, minSupportedVersion, maxSupportedVersion);
    }

    //1.7+ features
    //this is considered harmless enough we can keep doing it?
    fixed = CodeTransformations.removeAvroGeneratedAnnotation(fixed, minSupportedVersion, maxSupportedVersion);

    //1.8+ features
    fixed = CodeTransformations.removeBinaryMessageCodecSupport(fixed, minSupportedVersion, maxSupportedVersion);
    fixed = CodeTransformations.transformExternalizableSupport(fixed, minSupportedVersion, maxSupportedVersion, importStatements);

    //1.9+ features
    fixed = CodeTransformations.transformCustomCodersSupport(fixed, minSupportedVersion, maxSupportedVersion);

    //add any required imports
    fixed = CodeTransformations.addImports(fixed, importStatements);

    //general issues
    fixed = CodeTransformations.transformUnqalifiedCatchClauses(fixed);
    fixed = fixBuilderConstructors(fixed);

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
   * on top of all the above, we may want to substitute the schema literal being parsed with a modified one,
   * usually for bug-to-bug-compatibility with avro 1.4 reasons
   *
   * this method does 4 things:
   * <ul>
   *   <li>replaces all parse calls with Helper.parse() calls</li>
   *   <li>replaces giant literals inside the parse() calls with a StringBuilder</li>
   *   <li>properly escapes any control characters in string literals inside the avsc</li>
   *   <li>potentially replaces the schema literal with an alternative AVSC</li>
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
      AvroVersion maxSupportedVersion,
      String alternativeAvsc,
      Collection<String> importStatements
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

    //drop in alternative avsc?
    if (alternativeAvsc != null && !alternativeAvsc.isEmpty()) {
      stringLiteral = StringEscapeUtils.escapeJava(alternativeAvsc);
    } else if (generatedWith.earlierThan(AvroVersion.AVRO_1_6)) {
      // See https://github.com/linkedin/avro-util/issues/253. Avro 1.4 and 1.5 generate broken code when there are
      // escaped chars within strings. We undo the damage (unescape `\"` to `"`), and then fix it up properly (escape
      // `\` to `\\`, followed by `"` to `\"`).
      stringLiteral = stringLiteral.replace("\\\"", "\"")
          .replace("\\", "\\\\")
          .replace("\"", "\\\"");
    }

    boolean largeString = stringLiteral.length() >= MAX_STRING_LITERAL_SIZE;
    //either we've already been here, or modern avro was used that already emits vararg
    boolean ourVararg = stringLiteral.contains("new StringBuilder().append(");
    boolean avroVararg = PARSE_VARARG_PATTERN.matcher(stringLiteral).find();
    boolean alreadyVararg = ourVararg || avroVararg;

    String argToParseCall;
    if (largeString && !alreadyVararg) {
      List<String> pieces = SourceCodeUtils.safeSplit(stringLiteral, MAX_STRING_LITERAL_SIZE);
      StringBuilder argBuilder = new StringBuilder(stringLiteral.length()); //at least
      argBuilder.append("new StringBuilder()");
      for (String piece : pieces) {
        argBuilder.append(".append(\"").append(piece).append("\")");
      }
      argBuilder.append(".toString()");
      argToParseCall = argBuilder.toString();
    } else {
      argToParseCall = "\"" + stringLiteral + "\"";
    }

    String prefix = code.substring(0, startMatcher.start());
    importStatements.add(IMPORT_HELPER);
    String newParseCall = HelperConsts.HELPER_SIMPLE_NAME + ".parse(" + argToParseCall + ");";
    String restOfCode = code.substring(endMatcher.start() + 3);
    return prefix + newParseCall + restOfCode;
  }

  public static String transformParseCalls(
          String code,
          AvroVersion generatedWith,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion
  ) {
    return transformParseCalls(code, generatedWith, minSupportedVersion, maxSupportedVersion, null);
  }

  public static String transformParseCalls(
          String code,
          AvroVersion generatedWith,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion,
          String alternativeAvsc
  ) {
    Set<String> importStatements = new HashSet<>(1);
    String processed = transformParseCalls(code, generatedWith, minSupportedVersion, maxSupportedVersion, alternativeAvsc, importStatements);
    return CodeTransformations.addImports(processed, importStatements);
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
    if (!endBuilderMatcher.find(buildMethodMatcher.end())) {
      throw new IllegalStateException("cant locate builder support block in " + code);
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    String codeWithoutBuilder = code.substring(0, methodStart) + code.substring(endBuilderMatcher.end());
    return codeWithoutBuilder;
  }

  /**
   * avro 1.9+ builders directly reference some classes that only exist in avro 1.9+.
   * a concrete example of this is they have a catch clause for org.apache.avro.AvroMissingFieldException,
   * which does not exist prior to 1.9.
   *
   * @param code
   * @param generatedBy
   * @param minSupportedVersion
   * @param maxSupportedVersion
   * @return
   */
  public static String removeReferencesToNewClassesFromBuilders(
          String code,
          AvroVersion generatedBy,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion
  ) {
    if (generatedBy.earlierThan(AvroVersion.AVRO_1_9)) {
      return code; //nothing to strip out
    }
    if (minSupportedVersion.laterThan(AvroVersion.AVRO_1_8)) {
      return code; //everything mentioned in the builders should exist at runtime
    }

    String fixed = code;

    //replace catch (FieldException) {} + catch (Exception) combos at the end of build()
    Matcher startMatcher = CATCH_MISSING_FIELD_AND_REGULAR_EXCEPTION_PATTERN.matcher(fixed);
    while (startMatcher.find()) {
      int start = startMatcher.start();
      int end = startMatcher.end();
      fixed = fixed.substring(0, start) + COMPATIBLE_CATCH_REPLACEMENT + fixed.substring(end);
      startMatcher = CATCH_MISSING_FIELD_AND_REGULAR_EXCEPTION_PATTERN.matcher(fixed);
    }

    //replace any remaining stand-alone catch (FieldException) constructs
    startMatcher = CATCH_MISSING_FIELD_EXCEPTION_START_PATTERN.matcher(fixed);
    while (startMatcher.find()) {
      int start = startMatcher.start();
      Matcher endMatcher = CATCH_MISSING_FIELD_EXCEPTION_END_PATTERN.matcher(fixed);
      if (!endMatcher.find(start)) {
        throw new IllegalStateException("unable to find end of catch clause around " + fixed.substring(start, Math.min(fixed.length(), start + 300)));
      }
      int end = endMatcher.end();
      fixed = fixed.substring(0, start) + COMPATIBLE_CATCH_REPLACEMENT + fixed.substring(end);
      startMatcher = CATCH_MISSING_FIELD_EXCEPTION_START_PATTERN.matcher(fixed);
    }

    return fixed;
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
   * MODEL$ was introduced as a static field in the generated class, starting 1.8, which indicates the conversions
   * applicable for logical types in a specific record. An example of generated code looks like:
   * <pre>
   * {@code
   *   private static SpecificData MODEL$ = new SpecificData();
   * static {
   *     MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
   *   }
   * }
   * </pre>
   * However, starting 1.9, avro uses reflection to look up this
   * field, which will throw a {@link ReflectiveOperationException} exception for records generated from older version.
   * This results in  performance degradation.
   * Moreover, older avro runtime will not have classes used in these conversions.
   *
   * This methods avoids the exception by introducing this field in older versions of the generated record.
   *
   * @param code avro generated code that may or may not have the MODEL$ declaration
   * @param minSupportedVersion lowest avro version under which the generated code should work
   * @param maxSupportedVersion highest avro version under which the generated code should work
   * @param importStatements collection of java import statements that would need to be added to the resulting code
   * @return code where MODEL$ exists for avro versions expecting it at runtime (&gt;= 1.9)
   */
  public static String pacifyModel$Declaration(
          String code,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion,
          Collection<String> importStatements
  ) {
    Matcher match = MODEL_DECL_PATTERN.matcher(code);
    if (match.find()) {
      if (minSupportedVersion.earlierThan(AvroVersion.AVRO_1_8)) {  // minAvro < 1.8
        // replace MODEL$ with SpecificData.get and remove any static block after that contains any
        // record-specific type conversions
        // we dont need to add an import statement for SpecificData because the input
        // would already have one if we found the vanilla declaration
        code = match.replaceAll(Matcher.quoteReplacement(MODEL_DECL_REPLACEMENT));
        return MODEL_ADD_TYPE_CONVERSION_PATTERN.matcher(code).replaceAll("");
      }
      return code;
    } else {
      if (maxSupportedVersion.laterThan(AvroVersion.AVRO_1_8)) {  // maxAvro >= 1.9
        // add no-op MODEL$ to the end of the generated schema$ string
        // we also need an import statement for this
        importStatements.add(IMPORT_SPECIFICDATA);
        int schema$EndPosition = findEndOfSchemaDeclaration(code);
        return code.substring(0, schema$EndPosition) + "\n " + MODEL_DECL_REPLACEMENT + "\n " + code.substring(
            schema$EndPosition);
      }
      // do nothing - not intended for use under avro that cares about MODEL$
      return code;
    }
  }

  public static String pacifyModel$Declaration(
          String code,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion
  ) {
    Set<String> importStatements = new HashSet<>(1);
    String processed = pacifyModel$Declaration(code, minSupportedVersion, maxSupportedVersion, importStatements);
    return CodeTransformations.addImports(processed, importStatements);
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
   * @param importStatements collection of java import statements that would need to be added to the resulting code
   * @return code where the externalizable support still exists but is compatible with earlier avro at runtime
   */
  public static String transformExternalizableSupport(
          String code,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion,
          Collection<String> importStatements
  ) {
    //handle MODEL$ based on supported versions
    String codeWithoutModel = pacifyModel$Declaration(code, minSupportedVersion, maxSupportedVersion, importStatements);
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
    if (!withHelperCall.equals(withoutAnnotations)) {
      importStatements.add(IMPORT_HELPER);
    }

    return withHelperCall;
  }

  public static String transformExternalizableSupport(
          String code,
          AvroVersion minSupportedVersion,
          AvroVersion maxSupportedVersion
  ) {
    Set<String> importStatements = new HashSet<>(1);
    String processed = transformExternalizableSupport(code, minSupportedVersion, maxSupportedVersion, importStatements);
    return CodeTransformations.addImports(processed, importStatements);
  }

  /**
   * avro 1.9 introduced 3 new methods to {@link org.apache.avro.specific.SpecificRecordBase}:
   * <ul>
   *   <li>protected boolean hasCustomCoders()</li>
   *   <li>public void customEncode(Encoder)</li>
   *   <li>public void customDecode(ResolvingDecoder)</li>
   * </ul>
   *
   * the implementation of customDecode() relies on ResolvingDecoder.readFieldOrderIfDiff()
   * which only exists in avro 1.9, so under older avro we strip it all out.
   *
   * also, for records with too many fields, customDecode() can get big enough to prevent the
   * generated class from even compiling (see https://issues.apache.org/jira/browse/AVRO-2796)
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

    String codersSupportCode = code.substring(startMatcher.start(), endMatcher.end());

    boolean stripOut = false;
    if (minSupportedVersion.earlierThan(AvroVersion.AVRO_1_9)) {
      //strip it out because it would reference ResolvingDecoder.readFieldOrderIfDiff()
      //TODO - we can replace with ResolvingDecoder and some extra logic to make code portable
      stripOut = true;
    } else if (codersSupportCode.length() > 150000) {
      //if the code isnt big enough to trip AVRO-2796 we can leave it as-is, otherwise we strip it out
      //the threshold value we use is far from being an exact number. in local testing we have generated
      //code up to 320K characters that still produces compilable classes.
      stripOut = true;
    }

    if (!stripOut) {
      return code;
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    String codeWithout = code.substring(0, startMatcher.start()) + "\n" + code.substring(endMatcher.end());
    return codeWithout;
  }

  /**
   * older avro may generate code with "catch (Exception e)" clauses. since this doesnt qualify "Exception"
   * it might accidentally match a class called Exception in the same package.
   *
   * this method replaces any such unqualified catch clauses with fully-qualified "java.lang.Exception"
   * @param code code with potentially unqualified catch clauses
   * @return code with fully-qualified catch clauses
   */
  public static String transformUnqalifiedCatchClauses(String code) {
    Matcher match = CATCH_UNQUALIFIED_EXCEPTION_PATTERN.matcher(code);
    if (match.find()) {
      return match.replaceAll(Matcher.quoteReplacement(CATCH_QUALIFIED_EXCEPTION));
    }
    return code;
  }

  // We want to transform SpecificRecords that look like this:
  //     public class SomeRecord ... {
  //         ...
  //         public static class Builder ... {
  //             private Builder() {
  //                 super(SCHEMA$);
  //             }
  //             private Builder(SomeRecord other) {
  //                 super(SCHEMA$);
  //                 ...
  //             }
  // Into this:
  //     public class SomeRecord ... {
  //         ...
  //         private static final BUILDER_INSTANCE$ = new Builder(false);
  //         public static class Builder ... {
  //             private Builder(boolean unused) {
  //                 super(SCHEMA$);
  //             }
  //             private Builder() {
  //                 this(BUILDER_INSTANCE$);
  //             }
  //             private Builder(SomeRecord other) {
  //                 this(BUILDER_INSTANCE$);
  //                 ...
  //             }
  // See https://github.com/linkedin/avro-util/issues/220 for an explanation of why. Briefly:
  // * `super(SCHEMA$)` is very expensive when running under Avro 1.9 and 1.10.
  // * So, we call it once per SpecificRecord and cache the result in BUILDER_INSTANCE$.
  // * There's a `private Builder(Builder other)` constructor not shown above; it's more performant.
  // * Make the other constructors call that instead, using the cached builder instance.
  //
  // Avro 1.11 codegen produces Builder constructors that call `super(SCHEMA$, MODEL$)`. Such a method doesn't exist in
  // the super-class (SpecificRecordBuilderBase) in any older Avro. So, we transform those as well. This has the benefit
  // that Avro 1.11 codegen (post-processed by us) can still be used with runtime Avro 1.6 through 1.10.
  private static String fixBuilderConstructors(String code) {
    Matcher builderClassMatcher = BUILDER_CLASS_PATTERN.matcher(code);
    if (!builderClassMatcher.find()) {
      // There is no Builder in this code. Nothing to fix up.
      return code;
    }
    // Introduce a new constructor that calls the expensive `super(SCHEMA$)` method.
    // It is intended that this super() will NOT be matched by BUILDER_SUPER_PATTERN below.
    code = code.substring(0, builderClassMatcher.end()) +
        "\nprivate Builder(boolean unused) { super(SCHEMA$); }" +
        code.substring(builderClassMatcher.end());

    // Replace all the `super(SCHEMA$)` and `super(SCHEMA$, MODEL$)` calls with `this(BUILDER_INSTANCE$)`.
    Matcher builderSuperMatcher = BUILDER_SUPER_PATTERN.matcher(code);
    code = builderSuperMatcher.replaceAll(BUILDER_SUPER_REPLACEMENT);

    // Get the type name of the Builder. It may be prefixed by the package, which is why we can't just use `Builder`.
    builderClassMatcher = NEW_BUILDER_METHOD_PATTERN.matcher(code);
    if (!builderClassMatcher.find()) {
      throw new IllegalStateException("cannot locate newBuilder() method in " + code);
    }
    String builderClassName = builderClassMatcher.group(1);

    // Add the cached instance. It's important to add it after the SCHEMA$ and MODEL$ definitions, since the creation
    // of the cached instance may try to look up those fields.
    int insertPosition = code.indexOf(MODEL_DECL_REPLACEMENT);
    if (insertPosition < 0) {
      // Perhaps the MODEL$ definition was NOT transformed, for whatever reason. Look for the original definition.
      Matcher modelMatcher = MODEL_DECL_PATTERN.matcher(code);
      if (modelMatcher.find()) {
        insertPosition = modelMatcher.end();
      } else {
        // This must be because the max targeted Avro version is <= 1.8, which doesn't use MODEL$. Use SCHEMA$ instead.
        insertPosition = findEndOfSchemaDeclaration(code);
      }
    } else {
      insertPosition += MODEL_DECL_REPLACEMENT.length();
    }
    return code.substring(0, insertPosition) + "\nprivate static final " + builderClassName +
        " " + BUILDER_INSTANCE_NAME + " = new " + builderClassName + "(false);\n" + code.substring(insertPosition);
  }

  /**
   * Transforms the put method in Avro-generated record classes to allow setting Long values into int fields
   * and Integer values into long fields. This improves type compatibility when working with numeric fields.
   * This method preserves existing case statements and only enhances the ones for numeric fields.
   *
   * @param code generated code
   * @return transformed code with enhanced put method
   */
  public static String enhanceNumericPutMethod(String code) {
    if (code == null || code.isEmpty()) {
      return code;
    }

    // Pattern to match the put method with simple casts
    Pattern putMethodPattern = Pattern.compile(
        "public\\s+void\\s+put\\s*\\(\\s*int\\s+field\\$\\s*,\\s*java\\.lang\\.Object\\s+value\\$\\s*\\)\\s*\\{\\s*" +
        "switch\\s*\\(\\s*field\\$\\s*\\)\\s*\\{\\s*" +
        "([^}]+)" +  // Capture the case statements
        "\\}\\s*\\}"
    );

    Matcher matcher = putMethodPattern.matcher(code);
    if (!matcher.find()) {
      return code; // No matching put method found
    }

    String caseStatements = matcher.group(1);

    // Pattern to find int and long field cases
    Pattern casePattern = Pattern.compile("case\\s+(\\d+)\\s*:\\s*(\\w+)\\s*=\\s*\\(([^)]+)\\)value\\$\\s*;\\s*break\\s*;");
    Matcher caseMatcher = casePattern.matcher(caseStatements);

    // Map to store field info: caseNumber -> [fieldName, fieldType]
    Map<String, String[]> fieldInfo = new HashMap<>(); // fieldName -> type

    // Find all numeric field cases
    while (caseMatcher.find()) {
      String caseNumber = caseMatcher.group(1);
      String fieldName = caseMatcher.group(2);
      String fieldType = caseMatcher.group(3);

      if ("java.lang.Integer".equals(fieldType) || "int".equals(fieldType) ||
          "java.lang.Long".equals(fieldType) || "long".equals(fieldType)) {
        fieldInfo.put(caseNumber, new String[]{fieldName, fieldType});
      }
    }

    if (fieldInfo.isEmpty()) {
      return code; // No numeric fields found
    }

    // Build the enhanced put method, preserving existing case statements
    StringBuilder enhancedPutMethod = new StringBuilder();
    enhancedPutMethod.append("public void put(int field$, java.lang.Object value$) {\n");
    enhancedPutMethod.append("  switch (field$) {\n");

    // Reset the matcher to process all case statements
    caseMatcher = Pattern.compile("case\\s+(\\d+)\\s*:([^;]*);\\s*break\\s*;").matcher(caseStatements);
    int lastMatchEnd = 0;

    while (caseMatcher.find()) {
      String caseNumber = caseMatcher.group(1);

      // If this is a numeric field case that we want to enhance
      if (fieldInfo.containsKey(caseNumber)) {
        String[] fieldData = fieldInfo.get(caseNumber);
        String fieldName = fieldData[0];
        String fieldType = fieldData[1];

        enhancedPutMethod.append("  case ").append(caseNumber).append(": ");

        if ("int".equals(fieldType)) {
          enhancedPutMethod.append("if (value$ instanceof java.lang.Long) {\n");
          enhancedPutMethod.append("      if ((java.lang.Long)value$ <= Integer.MAX_VALUE && (java.lang.Long)value$ >= Integer.MIN_VALUE) {\n");
          enhancedPutMethod.append("        this.").append(fieldName).append(" = ((java.lang.Long)value$).intValue();\n");
          enhancedPutMethod.append("      } else {\n");
          enhancedPutMethod.append("        throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value$ + \" cannot be cast to int\");\n");
          enhancedPutMethod.append("      }\n");
          enhancedPutMethod.append("    } else {\n");
          enhancedPutMethod.append("      this.").append(fieldName).append(" = (java.lang.Integer)value$;\n");
          enhancedPutMethod.append("    }\n");
          enhancedPutMethod.append("    break;\n");
        } else if ("long".equals(fieldType)) {
          enhancedPutMethod.append("if (value$ instanceof java.lang.Integer) {\n");
          enhancedPutMethod.append("      this.").append(fieldName).append(" = ((java.lang.Integer)value$).longValue();\n");
          enhancedPutMethod.append("    } else {\n");
          enhancedPutMethod.append("      this.").append(fieldName).append(" = (java.lang.Long)value$;\n");
          enhancedPutMethod.append("    }\n");
          enhancedPutMethod.append("    break;\n");
        } else if ("java.lang.Integer".equals(fieldType)) {
          enhancedPutMethod.append("if (value$ instanceof java.lang.Long) {\n");
          enhancedPutMethod.append("      if ((java.lang.Long)value$ <= Integer.MAX_VALUE && (java.lang.Long)value$ >= Integer.MIN_VALUE) {\n");
          enhancedPutMethod.append("        this.").append(fieldName).append(" = ((java.lang.Long)value$).intValue();\n");
          enhancedPutMethod.append("      } else {\n");
          enhancedPutMethod.append("        throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value$ + \" cannot be cast to Integer\");\n");
          enhancedPutMethod.append("      }\n");
          enhancedPutMethod.append("    } else {\n");
          enhancedPutMethod.append("      this.").append(fieldName).append(" = (java.lang.Integer)value$;\n");
          enhancedPutMethod.append("    }\n");
          enhancedPutMethod.append("    break;\n");
        } else if ("java.lang.Long".equals(fieldType)) {
          enhancedPutMethod.append("if (value$ instanceof java.lang.Integer) {\n");
          enhancedPutMethod.append("      this.").append(fieldName).append(" = ((java.lang.Integer)value$).longValue();\n");
          enhancedPutMethod.append("    } else {\n");
          enhancedPutMethod.append("      this.").append(fieldName).append(" = (java.lang.Long)value$;\n");
          enhancedPutMethod.append("    }\n");
          enhancedPutMethod.append("    break;\n");
        }
      } else {
        // For non-numeric fields, preserve the original case statement
        enhancedPutMethod.append("  case ").append(caseNumber).append(":")
                         .append(caseMatcher.group(2)).append(";")
                         .append(" break;\n");
      }

      lastMatchEnd = caseMatcher.end();
    }

    // Add any remaining case statements (like default case)
    if (lastMatchEnd < caseStatements.length()) {
      String remaining = caseStatements.substring(lastMatchEnd).trim();
      if (!remaining.isEmpty()) {
        enhancedPutMethod.append("  ").append(remaining).append("\n");
      }
    } else {
      // Add the default case if it wasn't in the original
      enhancedPutMethod.append("  default: throw new IndexOutOfBoundsException(\"Invalid index: \" + field$);\n");
    }

    enhancedPutMethod.append("  }\n}");

    // Replace the original put method with the enhanced one
    return code.substring(0, matcher.start()) + enhancedPutMethod + code.substring(matcher.end());
  }

  /**
   * Enhances setter methods for numeric types (int, long, Integer, Long) to allow cross-type assignments.
   * This allows int/Integer fields to be set using long/Long values and long/Long fields to be set using int/Integer values.
   * For int/Integer fields, a runtime check ensures the long/Long value fits within the int/Integer range.
   *
   * @param code generated code
   * @return transformed code with enhanced setter methods
   */
  public static String addOverloadedNumericSetterMethods(String code) {
    if (code == null || code.isEmpty()) {
      return code;
    }

    // First, identify all numeric fields in the class (both primitive and boxed)
    Map<String, String> fieldTypes = new HashMap<>(); // fieldName -> type

    // Pattern to match primitive field declarations
    Pattern primitiveFieldPattern = Pattern.compile("private\\s+(int|long)\\s+(\\w+)\\s*;");
    Matcher primitiveFieldMatcher = primitiveFieldPattern.matcher(code);

    while (primitiveFieldMatcher.find()) {
      String fieldType = primitiveFieldMatcher.group(1);
      String fieldName = primitiveFieldMatcher.group(2);
      fieldTypes.put(fieldName, fieldType);
    }

    // Pattern to match boxed field declarations
    Pattern boxedFieldPattern = Pattern.compile("private\\s+(java\\.lang\\.Integer|java\\.lang\\.Long)\\s+(\\w+)\\s*;");
    Matcher boxedFieldMatcher = boxedFieldPattern.matcher(code);

    while (boxedFieldMatcher.find()) {
      String fieldType = boxedFieldMatcher.group(1);
      String fieldName = boxedFieldMatcher.group(2);
      fieldTypes.put(fieldName, fieldType);
    }

    if (fieldTypes.isEmpty()) {
      return code; // No numeric fields found
    }

    StringBuilder result = new StringBuilder(code);

    // For each field, find and enhance its setter method
    for (Map.Entry<String, String> entry : fieldTypes.entrySet()) {
      String fieldName = entry.getKey();
      String fieldType = entry.getValue();

      // Convert field name to method name (camelCase to PascalCase)
      String methodName = "set" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

      // Pattern to match the setter method - handle both primitive and boxed types
      Pattern setterPattern;
      if ("int".equals(fieldType) || "long".equals(fieldType)) {
        setterPattern = Pattern.compile(
            "public\\s+void\\s+" + methodName + "\\s*\\(\\s*" + fieldType + "\\s+\\w+\\s*\\)\\s*\\{[^}]+\\}"
        );
      } else {
        setterPattern = Pattern.compile(
            "public\\s+void\\s+" + methodName + "\\s*\\(\\s*" + fieldType + "\\s+value\\s*\\)\\s*\\{\\s*" +
            "this\\." + fieldName + "\\s*=\\s*value;\\s*" +
            "\\}"
        );
      }

      Matcher setterMatcher = setterPattern.matcher(result);

      if (setterMatcher.find()) {
        StringBuilder enhancedSetter = new StringBuilder();

        if ("int".equals(fieldType)) {
          // For int fields, add an overloaded setter that accepts long with bounds checking
          enhancedSetter.append("    public void ").append(methodName).append("(long value) {\n");
          enhancedSetter.append("        if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {\n");
          enhancedSetter.append("            this.").append(fieldName).append(" = (int) value;\n");
          enhancedSetter.append("        } else {\n");
          enhancedSetter.append("            throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to int\");\n");
          enhancedSetter.append("        }\n");
          enhancedSetter.append("    }\n\n");

          enhancedSetter.append("    public void ").append(methodName).append("(int value) {\n");
          enhancedSetter.append("        this.").append(fieldName).append(" = value;\n");
          enhancedSetter.append("    }\n\n");
        } else if ("long".equals(fieldType)) {
          // For long fields, add an overloaded setter that accepts int
          enhancedSetter.append("    public void ").append(methodName).append("(long value) {\n");
          enhancedSetter.append("        this.").append(fieldName).append(" = value;\n");
          enhancedSetter.append("    }\n\n");

          enhancedSetter.append("    public void ").append(methodName).append("(int value) {\n");
          enhancedSetter.append("        this.").append(fieldName).append(" = value;\n");
          enhancedSetter.append("    }\n\n");
        } else if ("java.lang.Integer".equals(fieldType)) {
          // For Integer fields, add an overloaded setter that accepts Long with bounds checking
          enhancedSetter.append("    public void ").append(methodName).append("(java.lang.Integer value) {\n");
          enhancedSetter.append("        this.").append(fieldName).append(" = value;\n");
          enhancedSetter.append("    }\n\n");

          enhancedSetter.append("    public void ").append(methodName).append("(java.lang.Long value) {\n");
          enhancedSetter.append("        if (value == null) {\n");
          enhancedSetter.append("            this.").append(fieldName).append(" = null;\n");
          enhancedSetter.append("        } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {\n");
          enhancedSetter.append("            this.").append(fieldName).append(" = value.intValue();\n");
          enhancedSetter.append("        } else {\n");
          enhancedSetter.append("            throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to Integer\");\n");
          enhancedSetter.append("        }\n");
          enhancedSetter.append("    }\n\n");
        } else { // java.lang.Long
          // For Long fields, add an overloaded setter that accepts Integer
          enhancedSetter.append("    public void ").append(methodName).append("(java.lang.Long value) {\n");
          enhancedSetter.append("        this.").append(fieldName).append(" = value;\n");
          enhancedSetter.append("    }\n\n");

          enhancedSetter.append("    public void ").append(methodName).append("(java.lang.Integer value) {\n");
          enhancedSetter.append("        if (value == null) {\n");
          enhancedSetter.append("            this.").append(fieldName).append(" = null;\n");
          enhancedSetter.append("        } else {\n");
          enhancedSetter.append("            this.").append(fieldName).append(" = value.longValue();\n");
          enhancedSetter.append("        }\n");
          enhancedSetter.append("    }\n\n");
        }

        // Replace the original setter with the enhanced version
        result.replace(setterMatcher.start(), setterMatcher.end(), enhancedSetter.toString());
      }
    }

    return result.toString();
  }

  /**
   * Adds an overloaded constructor that allows Integer parameters for Long fields and Long parameters for Integer fields.
   * This makes the generated code more flexible by allowing automatic type conversion between numeric types.
   * The implementation handles both boxed Integer/Long types and primitive int/long types, with proper null handling
   * for boxed types and default values for primitives when null is passed.
   *
   * @param code generated code
   * @return transformed code with overloaded constructor for numeric type conversions
   */
  public static String addOverloadedNumericConstructor(String code) {
    if (code == null || code.isEmpty()) {
      return code;
    }

    // First, identify all numeric fields in the class (both primitive and boxed)
    Map<String, String> fieldTypes = new HashMap<>(); // fieldName -> type

    // Pattern to match primitive field declarations
    Pattern primitiveFieldPattern = Pattern.compile("private\\s+(int|long)\\s+(\\w+)\\s*;");
    Matcher primitiveFieldMatcher = primitiveFieldPattern.matcher(code);

    while (primitiveFieldMatcher.find()) {
      String fieldType = primitiveFieldMatcher.group(1);
      String fieldName = primitiveFieldMatcher.group(2);
      fieldTypes.put(fieldName, fieldType);
    }

    // Pattern to match boxed field declarations
    Pattern boxedFieldPattern = Pattern.compile("private\\s+(java\\.lang\\.Integer|java\\.lang\\.Long)\\s+(\\w+)\\s*;");
    Matcher boxedFieldMatcher = boxedFieldPattern.matcher(code);

    while (boxedFieldMatcher.find()) {
      String fieldType = boxedFieldMatcher.group(1);
      String fieldName = boxedFieldMatcher.group(2);
      fieldTypes.put(fieldName, fieldType);
    }

    if (fieldTypes.isEmpty()) {
      return code; // No numeric fields found
    }

    // Find the class name
    Pattern classNamePattern = Pattern.compile("public\\s+class\\s+(\\w+)");
    Matcher classNameMatcher = classNamePattern.matcher(code);
    if (!classNameMatcher.find()) {
      return code; // Can't find class name
    }
    String className = classNameMatcher.group(1);

    // Find the all-args constructor
    Pattern constructorPattern = Pattern.compile(
        "public\\s+" + className + "\\s*\\(([^)]*)\\)\\s*\\{([^}]*)\\}"
    );
    Matcher constructorMatcher = constructorPattern.matcher(code);

    // Try to find a constructor with parameters
    boolean foundConstructor = false;
    String constructorParams = "";
    int constructorEnd = -1;
    
    // Skip the default constructor (no args) if present
    if (constructorMatcher.find()) {
      String params = constructorMatcher.group(1).trim();
      if (!params.isEmpty()) {
        // Found a constructor with parameters
        foundConstructor = true;
        constructorParams = params;
        constructorEnd = constructorMatcher.end();
      } else {
        // Found a default constructor, try to find another constructor with parameters
        if (constructorMatcher.find()) {
          foundConstructor = true;
          constructorParams = constructorMatcher.group(1).trim();
          constructorEnd = constructorMatcher.end();
        }
      }
    }
    
    // If no constructor with parameters was found, return the original code
    if (!foundConstructor || constructorParams.isEmpty()) {
      return code;
    }

    // Parse the constructor parameters using a more robust approach that handles generic types
    List<String> paramTypes = new ArrayList<>();
    List<String> paramNames = new ArrayList<>();
    Map<String, String> originalParamTypes = new HashMap<>(); // paramName -> original type

    // Parse parameters more carefully to handle generics
    parseConstructorParameters(constructorParams, paramTypes, paramNames, originalParamTypes);

    // Generate the overloaded constructor with swapped numeric types
    StringBuilder overloadedConstructor = new StringBuilder();
    overloadedConstructor.append("\n  /**\n");
    overloadedConstructor.append("   * All-args constructor with flexible numeric type conversion.\n");
    overloadedConstructor.append("   * Allows Integer parameters for Long fields and Long parameters for Integer fields.\n");

    // Add parameter javadoc
    for (int i = 0; i < paramNames.size(); i++) {
      overloadedConstructor.append("   * @param ").append(paramNames.get(i)).append(" The new value for ").append(paramNames.get(i)).append("\n");
    }

    overloadedConstructor.append("   */\n");
    overloadedConstructor.append("  public ").append(className).append("(");

    // Generate parameters with swapped types for numeric fields
    Map<String, String> swappedParamTypes = new HashMap<>(); // paramName -> swapped type

    for (int i = 0; i < paramTypes.size(); i++) {
      if (i > 0) {
        overloadedConstructor.append(", ");
      }

      String paramType = paramTypes.get(i);
      String paramName = paramNames.get(i);
      String fieldType = fieldTypes.get(paramName);

      // Swap Java types for numeric fields
      if ("java.lang.Long".equals(paramType) && fieldTypes.containsKey(paramName)) {
        overloadedConstructor.append("java.lang.Integer ").append(paramName);
        swappedParamTypes.put(paramName, "java.lang.Integer");
      } else if ("java.lang.Integer".equals(paramType) && fieldTypes.containsKey(paramName)) {
        overloadedConstructor.append("java.lang.Long ").append(paramName);
        swappedParamTypes.put(paramName, "java.lang.Long");
      } else {
        overloadedConstructor.append(paramType).append(" ").append(paramName);
        swappedParamTypes.put(paramName, paramType);
      }
    }

    overloadedConstructor.append(") {\n");

    // Generate constructor body with type conversion logic
    for (int i = 0; i < paramNames.size(); i++) {
      String paramName = paramNames.get(i);
      String fieldType = fieldTypes.get(paramName);
      String swappedParamType = swappedParamTypes.get(paramName);

      if (fieldType != null) {
        if ("int".equals(fieldType) && "java.lang.Long".equals(swappedParamType)) {
          // Convert Long to int with bounds check
          overloadedConstructor.append("    if (").append(paramName).append(" == null) {\n");
          overloadedConstructor.append("      this.").append(paramName).append(" = 0;\n");
          overloadedConstructor.append("    } else if (").append(paramName).append(" <= Integer.MAX_VALUE && ")
                               .append(paramName).append(" >= Integer.MIN_VALUE) {\n");
          overloadedConstructor.append("      this.").append(paramName).append(" = ").append(paramName).append(".intValue();\n");
          overloadedConstructor.append("    } else {\n");
          overloadedConstructor.append("      throw new org.apache.avro.AvroRuntimeException(\"Long value \" + ")
                               .append(paramName).append(" + \" cannot be cast to int\");\n");
          overloadedConstructor.append("    }\n");
        } else if ("long".equals(fieldType) && "java.lang.Integer".equals(swappedParamType)) {
          // Convert Integer to long
          overloadedConstructor.append("    this.").append(paramName).append(" = ").append(paramName).append(" == null ? 0L : ")
                               .append(paramName).append(".longValue();\n");
        } else if ("java.lang.Integer".equals(fieldType) && "java.lang.Long".equals(swappedParamType)) {
          // Convert Long to Integer with bounds check
          overloadedConstructor.append("    if (").append(paramName).append(" == null) {\n");
          overloadedConstructor.append("      this.").append(paramName).append(" = null;\n");
          overloadedConstructor.append("    } else if (").append(paramName).append(" <= Integer.MAX_VALUE && ")
                               .append(paramName).append(" >= Integer.MIN_VALUE) {\n");
          overloadedConstructor.append("      this.").append(paramName).append(" = ").append(paramName).append(".intValue();\n");
          overloadedConstructor.append("    } else {\n");
          overloadedConstructor.append("      throw new org.apache.avro.AvroRuntimeException(\"Long value \" + ")
                               .append(paramName).append(" + \" cannot be cast to Integer\");\n");
          overloadedConstructor.append("    }\n");
        } else if ("java.lang.Long".equals(fieldType) && "java.lang.Integer".equals(swappedParamType)) {
          // Convert Integer to Long
          overloadedConstructor.append("    if (").append(paramName).append(" == null) {\n");
          overloadedConstructor.append("      this.").append(paramName).append(" = null;\n");
          overloadedConstructor.append("    } else {\n");
          overloadedConstructor.append("      this.").append(paramName).append(" = ").append(paramName).append(".longValue();\n");
          overloadedConstructor.append("    }\n");
        } else {
          // For non-numeric fields or fields with the same type, just assign directly
          overloadedConstructor.append("    this.").append(paramName).append(" = ").append(paramName).append(";\n");
        }
      } else {
        // For fields not in our fieldTypes map, just assign directly
        overloadedConstructor.append("    this.").append(paramName).append(" = ").append(paramName).append(";\n");
      }
    }

    overloadedConstructor.append("  }");

    // Insert the overloaded constructor after the original constructor
    StringBuilder result = new StringBuilder(code);
    result.insert(constructorEnd, "\n" + overloadedConstructor.toString());

    return result.toString();
  }

  /**
   * Parses constructor parameters handling complex generic types correctly.
   * This method properly handles nested generics like Map<String, List<String>>.
   *
   * @param constructorParams The constructor parameter string
   * @param paramTypes Output list to store parameter types
   * @param paramNames Output list to store parameter names
   * @param originalParamTypes Output map to store parameter name to type mapping
   */
  private static void parseConstructorParameters(String constructorParams,
                                               List<String> paramTypes,
                                               List<String> paramNames,
                                               Map<String, String> originalParamTypes) {
    if (constructorParams == null || constructorParams.trim().isEmpty()) {
      return;
    }

    int pos = 0;
    int len = constructorParams.length();
    int angleNestLevel = 0;
    int startPos = 0;

    while (pos < len) {
      char c = constructorParams.charAt(pos);

      if (c == '<') {
        angleNestLevel++;
      } else if (c == '>') {
        angleNestLevel--;
      } else if (c == ',' && angleNestLevel == 0) {
        // Found a top-level comma, which separates parameters
        String param = constructorParams.substring(startPos, pos).trim();
        processParameter(param, paramTypes, paramNames, originalParamTypes);
        startPos = pos + 1;
      }

      pos++;
    }

    // Process the last parameter
    if (startPos < len) {
      String param = constructorParams.substring(startPos).trim();
      processParameter(param, paramTypes, paramNames, originalParamTypes);
    }
  }

  /**
   * Processes a single parameter string and extracts the type and name.
   *
   * @param param The parameter string (e.g., "java.lang.Integer fieldName")
   * @param paramTypes Output list to store parameter types
   * @param paramNames Output list to store parameter names
   * @param originalParamTypes Output map to store parameter name to type mapping
   */
  private static void processParameter(String param,
                                     List<String> paramTypes,
                                     List<String> paramNames,
                                     Map<String, String> originalParamTypes) {
    if (param == null || param.trim().isEmpty()) {
      return;
    }

    // Find the last space which separates the type from the name
    int lastSpacePos = param.lastIndexOf(' ');
    if (lastSpacePos > 0) {
      String paramType = param.substring(0, lastSpacePos).trim();
      String paramName = param.substring(lastSpacePos + 1).trim();

      paramTypes.add(paramType);
      paramNames.add(paramName);
      originalParamTypes.put(paramName, paramType);
    }
  }

  private static String addImports(String code, Collection<String> importStatements) {
    if (importStatements == null || importStatements.isEmpty()) {
      return code;
    }

    ArrayList<String> sortedImports = new ArrayList<>(importStatements);
    Collections.sort(sortedImports);

    //find where to put the imports:
    //after package (if there is a package)
    //before class-level annotations (if any)
    //before class-level comment (if there is one)
    //before the class/enum declaration (this must exist)

    int lowerBound = 0;
    int upperBound;

    Matcher classMatcher = CLASS_PATTERN.matcher(code);
    if (!classMatcher.find()) {
      //this isnt a record class. must be an enum
      classMatcher = ENUM_PATTERN.matcher(code);
      if (!classMatcher.find()) {
        throw new IllegalStateException("unable to find class or enum declaration in " + code);
      }
    }
    upperBound = classMatcher.start();

    Matcher packageMatcher = PACKAGE_PATTERN.matcher(code);
    if (packageMatcher.find() && packageMatcher.start() < classMatcher.start()) {
      //found package declaration. adjust lowerBound
      lowerBound = Math.max(lowerBound, packageMatcher.end());
    }

    Matcher commentMatcher = COMMENT_PATTERN.matcher(code);
    if (commentMatcher.find() && commentMatcher.start() < classMatcher.start() && commentMatcher.start() >= lowerBound) {
      //found a class-level comment (that hopefully isnt a license header ...). adjust upperBound
      upperBound = Math.min(upperBound, commentMatcher.start());
    }

    Matcher annotationMatcher = ANNOTATION_PATTERN.matcher(code);
    if (annotationMatcher.find() && annotationMatcher.start() < classMatcher.start()) {
      //found class-level annotation. adjust upper bound
      upperBound = Math.min(upperBound, annotationMatcher.start());
    }

    //find any existing imports between the bounds
    //also keep track of end of imports block to add ours after
    Matcher importMatcher = IMPORT_PATTERN.matcher(code);
    int endOfImports = lowerBound;
    int prevHit = lowerBound - 1;
    while (importMatcher.find(prevHit + 1)) {
      if (importMatcher.start() >= upperBound) {
        break;
      }
      String existingImport = importMatcher.group();
      sortedImports.remove(existingImport);
      endOfImports = importMatcher.end();
      prevHit = endOfImports - 1;
    }

    if (sortedImports.isEmpty()) {
      return code;
    }

    StringJoiner joiner = new StringJoiner("\n");
    for (String importStatement : sortedImports) {
      joiner.add(importStatement);
    }
    String newImports = joiner.toString();
    return code.substring(0, endOfImports) + "\n" + newImports + "\n" + code.substring(endOfImports);
  }
}
