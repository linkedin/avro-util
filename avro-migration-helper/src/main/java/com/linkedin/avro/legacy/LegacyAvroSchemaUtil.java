/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package com.linkedin.avro.legacy;

import com.linkedin.avro.util.AvroSchemaUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.JsonDecoder;
import org.json.JSONException;
import org.json.JSONObject;


//package-private ON PURPOSE
class LegacyAvroSchemaUtil {
  private static final int MAX_STEPS_ATTEMPTED = 10;
  private static final Set<String> ILLEGAL_CHAR_PREFIXES =
      new HashSet<>(Arrays.asList("Illegal character in: ", "Illegal initial character: "));
  private static final String DUPLICATE_FIELD_PREFIX = "Duplicate field ";
  private static final Pattern INVALID_DEFAULT_FIELD_PATTERN =
      Pattern.compile("Invalid default for field (.*): (.*) not a \"(.*)\"");
  private static final Pattern UNKNOWN_UNION_BRANCH_PATTERN = Pattern.compile("Unknown union branch\\s+(.*)\\s*");
  private static final Set<Schema.Type> NAMED_TYPES = new HashSet<>(
      Arrays.asList(Schema.Type.ENUM, Schema.Type.RECORD, Schema.Type.FIXED)
  );

  private LegacyAvroSchemaUtil() {
    //util class, dont build it
  }

  static GenericRecord deserializeJson(LegacyAvroSchema schema, String json) {
    String transformedJson = json;

    //if we had to apply any transforms to the schema to fix it (like escape illegal chars in identifiers)
    //we need to apply those same transformations to the json object to make it readable.
    for (SchemaTransformStep transform : schema.getTransforms()) {
      transformedJson = transform.applyToJsonObject(transformedJson);
    }

    List<PayloadTransformStep> stepsTaken = new ArrayList<>(1);
    while (true) {
      try {
        JsonDecoder decoder = AvroCompatibilityHelper.newJsonDecoder(schema.getFixedSchema(), transformedJson);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema.getFixedSchema());
        return reader.read(null, decoder);
      } catch (Exception issue) {
        if (stepsTaken.size() > MAX_STEPS_ATTEMPTED) {
          throw new IllegalArgumentException("unable to deserialize json");
        }

        PayloadTransformStep step = findFixFor(schema, issue);
        if (step == null) {
          //if we got here we have no idea what the issue is nor how to fix it
          throw new IllegalStateException("unhandled", issue);
        }
        String fixedJson = step.applyToJsonPayload(transformedJson);
        if (fixedJson.equals(transformedJson)) {
          throw new IllegalStateException("made no progress fixing json");
        }
        transformedJson = fixedJson;
        stepsTaken.add(step);
      }
    }
  }

  private static PayloadTransformStep findFixFor(LegacyAvroSchema schema, Exception issue) {
    String msg = String.valueOf(issue.getMessage());

    //is it a fqcn vs shortname in union values issue?
    //see https://issues.apache.org/jira/browse/AVRO-656
    Matcher matcher = UNKNOWN_UNION_BRANCH_PATTERN.matcher(msg);
    if (matcher.find()) {
      String identifier = matcher.group(1);
      Schema fixedSchema = schema.getFixedSchema();
      Map<String, String> replacements = new HashMap<>();
      if (identifier.contains(".")) {
        //json uses FQCNs in branches, runtime avro expects simple names
        AvroSchemaUtil.traverseSchema(fixedSchema, new UnionBranchMatchingVisitor(branchSchema -> {
          return NAMED_TYPES.contains(branchSchema.getType()) && identifier.equals(branchSchema.getFullName());
        }, match -> {
          //add a fqcn --> simple find-and-replace pair
          replacements.put(match.getFullName(), match.getName());
        }));
      } else {
        //json uses simple names, runtime avro expects FQCNs
        AvroSchemaUtil.traverseSchema(fixedSchema, new UnionBranchMatchingVisitor(branchSchema -> {
          return NAMED_TYPES.contains(branchSchema.getType()) && identifier.equals(branchSchema.getName());
        }, match -> {
          //add a simple --> fqcn find-and-replace pair
          replacements.put(match.getName(), match.getFullName());
        }));
      }
      return new PayloadFindAndReplaceStep(replacements);
    }

    return null;
  }

  /**
   * attempts to minify and fix up an avro schema definition json.
   * first we see if the input is valid json (and if so minify it).
   * if its valid json, we repeatedly try to parse it, catching the parse
   * exception and trying to fix the issue.
   * if at any point we are unable to fix the issue (because we hit a parse exception
   * we dont know how to fix or our attempted fix does nothing) we give up on fixing.
   * @param badAvro a (possibly malformed) avro schema definition (contents of *.avsc, for example)
   * @return a "struct" returning the minified json (if json), the fixed schema and list of steps
   * taken to fix it (if fixable) or the issue preventing any further progress (if unfixable)
   */
  static SchemaFixResults tryFix(String badAvro) {

    //1st step is to see if what we're given is valid json.
    //this step also strips out whitespaces carriage returns line breaks etc
    //which makes some operations significantly easier below
    String compactJson;
    String minified;
    try {
      compactJson = new JSONObject(badAvro).toString();
      minified = compactJson;
    } catch (JSONException e) {
      //this isnt proper json. give up
      return SchemaFixResults.notJson(badAvro, e);
    }

    List<SchemaTransformStep> stepsTaken =
        new ArrayList<>(1); //micro-optimization - we dont expect to perform a lot of steps
    while (true) {
      try {
        Schema fixed = AvroCompatibilityHelper.parse(compactJson); //compiles against avro 1.4 and 1.7
        if (stepsTaken.isEmpty()) {
          return SchemaFixResults.valid(badAvro, fixed); //schema was ok to begin with
        }
        return SchemaFixResults.fixable(badAvro, minified, fixed, stepsTaken); //success
      } catch (AvroRuntimeException issue) {
        if (stepsTaken.size() > MAX_STEPS_ATTEMPTED) {
          return SchemaFixResults.unfixable(badAvro, minified,
              issue); //give up. this is in case we end up going in loops
        }

        SchemaTransformStep step = findFixFor(issue);
        if (step == null) {
          //if we got here we have no idea what the issue is nor how to fix it
          return SchemaFixResults.unfixable(badAvro, minified, issue);
        }
        String fixedJson = step.applyToSchema(compactJson);
        if (fixedJson.equals(compactJson)) {
          return SchemaFixResults.unfixable(badAvro, minified, new IllegalStateException("made no progress in fixing " + issue.getMessage()));
        }
        compactJson = fixedJson;
        stepsTaken.add(step);
      }
    }
  }

  private static SchemaTransformStep findFixFor(AvroRuntimeException issue) {
    String msg = String.valueOf(issue.getMessage());

    //is it an illegal identifier issue? if so, whats the problematic identifier string?
    String illegalIdentifier = tryParseIllegalIdentifier(msg);
    if (illegalIdentifier != null) {
      String fixedIdentifier = escapeIllegalCharacters(illegalIdentifier);
      return new RenameIdentifierStep(issue, illegalIdentifier, fixedIdentifier);
    }

    //is it a duplicate field issue?
    String duplicateFieldName = tryParseDuplicateFieldname(msg);
    if (duplicateFieldName != null) {
      return new RemoveDuplicateStep(issue, duplicateFieldName);
    }

    //is it a case of invalid default field value?
    FixDefaultValueStep.BadDefaultPropertySpec badPropertySpec = tryParseBadPropertyDefault(msg);
    if (badPropertySpec != null) {
      return new FixDefaultValueStep(issue, badPropertySpec);
    }

    return null;
  }

  /**
   * given an avro exception message, see if its a known illegal character
   * message. if so, parse the problematic identifier string out of the message
   * @param exceptionMessage message from an avro exception
   * @return the problematic identifier, if message is a known illegal character issue
   */
  private static String tryParseIllegalIdentifier(String exceptionMessage) {
    for (String knownPrefix : ILLEGAL_CHAR_PREFIXES) {
      if (exceptionMessage.startsWith(knownPrefix)) {
        return exceptionMessage.substring(knownPrefix.length());
      }
    }
    return null; //not an illegal character issue
  }

  /**
   * given an avro exception message, see if its a known duplicate field declaration
   * message. if so, parse and return the problematic field name out of the message
   * @param exceptionMessage message from an avro exception
   * @return the problematic field, or null is message is not a dup. field issue.
   */
  private static String tryParseDuplicateFieldname(String exceptionMessage) {
    if (exceptionMessage.startsWith(DUPLICATE_FIELD_PREFIX)) {
      return exceptionMessage.substring(DUPLICATE_FIELD_PREFIX.length(), exceptionMessage.indexOf(" in record"));
    }
    return null; //not a duplicate field issue
  }

  /**
   * given an avro exception message, see if its a bad default value for a field.
   * if so, parse and return data about the field with the bad default value.
   * @param exceptionMessage message from an avro exception
   * @return the problematic property, or null if the message is not a bad default value.
   */
  private static FixDefaultValueStep.BadDefaultPropertySpec tryParseBadPropertyDefault(String exceptionMessage) {
    Matcher matcher = INVALID_DEFAULT_FIELD_PATTERN.matcher(exceptionMessage);
    if (matcher.matches()) {
      String fieldName = matcher.group(1);
      String badValue = matcher.group(2);
      String fieldType = matcher.group(3);
      return new FixDefaultValueStep.BadDefaultPropertySpec(fieldName, badValue, fieldType);
    }
    return null;
  }

  /**
   * replaces any characters that the avro specification does not allow in identifier names
   * with "_&lt;code&gt;_" where code is the character code for the escaped character.
   * this results in an identifier that should be valid according to the avro specification
   * @param input a problematic identifier
   * @return an identifier with the problematic characters escaped
   */
  public static String escapeIllegalCharacters(String input) {
    StringBuilder fixed = new StringBuilder();
    char c = input.charAt(0);
    //1st character is special
    if (!(Character.isLetter(c) || c == '_')) {
      escapeCharacter(c, fixed);
    } else {
      fixed.append(c); //pass-through
    }
    for (int i = 1; i < input.length(); i++) {
      c = input.charAt(i);
      if (!(Character.isLetterOrDigit(c) || c == '_')) {
        escapeCharacter(c, fixed);
      } else {
        fixed.append(c); //pass-through
      }
    }
    return fixed.toString();
  }

  private static void escapeCharacter(char c, StringBuilder into) {
    into.append("_").append(Integer.toString((int) c)).append("_");
  }
}
