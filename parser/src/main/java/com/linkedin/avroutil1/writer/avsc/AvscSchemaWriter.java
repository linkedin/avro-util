/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.writer.avsc;

import com.linkedin.avroutil1.model.AvroArrayLiteral;
import com.linkedin.avroutil1.model.AvroArraySchema;
import com.linkedin.avroutil1.model.AvroBooleanLiteral;
import com.linkedin.avroutil1.model.AvroBytesLiteral;
import com.linkedin.avroutil1.model.AvroDoubleLiteral;
import com.linkedin.avroutil1.model.AvroEnumLiteral;
import com.linkedin.avroutil1.model.AvroEnumSchema;
import com.linkedin.avroutil1.model.AvroFixedLiteral;
import com.linkedin.avroutil1.model.AvroFixedSchema;
import com.linkedin.avroutil1.model.AvroFloatLiteral;
import com.linkedin.avroutil1.model.AvroIntegerLiteral;
import com.linkedin.avroutil1.model.AvroLiteral;
import com.linkedin.avroutil1.model.AvroLongLiteral;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroName;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroNullLiteral;
import com.linkedin.avroutil1.model.AvroPrimitiveSchema;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroStringLiteral;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.JsonPropertiesContainer;
import com.linkedin.avroutil1.model.SchemaOrRef;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.stream.JsonGenerator;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;


public class AvscSchemaWriter implements AvroSchemaWriter {

  @Override
  public List<AvscFile> write(AvroSchema schema, AvscWriterConfig config) {
    String avsc = generateAvsc(schema, AvscWriterConfig.CORRECT_MITIGATED);
    Path relativeFileName = pathForSchema(schema); //null  of not a named schema
    AvscFile file = new AvscFile(schema, relativeFileName, avsc);
    return Collections.singletonList(file);
  }

  protected Path pathForSchema(AvroSchema maybeNamed) {
    if (!(maybeNamed instanceof AvroNamedSchema)) {
      return null;
    }
    AvroNamedSchema namedSchema = (AvroNamedSchema) maybeNamed;
    AvroName name = namedSchema.getName();

    if (!name.hasNamespace()) {
      return Paths.get(name.getSimpleName() + "." + AvscFile.SUFFIX);
    }
    String fullname = name.getFullname();
    String[] parts = fullname.split("\\.");
    String[] pathParts = new String[parts.length - 1];
    for (int i = 1; i < parts.length; i++) {
      if (i == parts.length - 1) {
        pathParts[i - 1] = parts[i] + "." + AvscFile.SUFFIX;
      } else {
        pathParts[i -1] = parts[i];
      }
    }

    return Paths.get(parts[0], pathParts);
  }

  protected String generateAvsc(AvroSchema schema, AvscWriterConfig config) {
    AvscWriterContext context = new AvscWriterContext();
    Map<String, String> jsonConfig = new HashMap<>();
    if (config.isPretty()) {
      jsonConfig.put(JsonGenerator.PRETTY_PRINTING, "true");
    }
    StringWriter stringWriter = new StringWriter();
    JsonValue dom = writeSchema(schema, context, config);
    JsonWriter writer = Json.createWriterFactory(jsonConfig).createWriter(stringWriter);
    writer.write(dom);
    return stringWriter.toString();
  }

  protected JsonValue writeSchema(AvroSchema schema, AvscWriterContext context, AvscWriterConfig config) {
    AvroType type = schema.type();
    JsonObjectBuilder definitionBuilder;
    switch (type) {
      case ENUM:
      case FIXED:
      case RECORD:
        return writeNamedSchema((AvroNamedSchema) schema, context, config);
      case ARRAY:
        AvroArraySchema arraySchema = (AvroArraySchema) schema;
        definitionBuilder = Json.createObjectBuilder();
        definitionBuilder.add("type", "array");
        definitionBuilder.add("items", writeSchema(arraySchema.getValueSchema(), context, config));
        emitJsonProperties(schema, context, config, definitionBuilder);
        return definitionBuilder.build();
      case MAP:
        AvroMapSchema mapSchema = (AvroMapSchema) schema;
        definitionBuilder = Json.createObjectBuilder();
        definitionBuilder.add("type", "map");
        definitionBuilder.add("values", writeSchema(mapSchema.getValueSchema(), context, config));
        emitJsonProperties(schema, context, config, definitionBuilder);
        return definitionBuilder.build();
      case UNION:
        AvroUnionSchema unionSchema = (AvroUnionSchema) schema;
        JsonArrayBuilder unionBuilder = Json.createArrayBuilder();
        for (SchemaOrRef unionBranch : unionSchema.getTypes()) {
          AvroSchema branchSchema = unionBranch.getSchema(); //will throw if unresolved ref
          unionBuilder.add(writeSchema(branchSchema, context, config));
        }
        return unionBuilder.build();
      default:
        AvroPrimitiveSchema primitiveSchema = (AvroPrimitiveSchema) schema;
        if (!primitiveSchema.hasProperties()) {
          return Json.createValue(primitiveSchema.type().name().toLowerCase(Locale.ROOT));
        }
        definitionBuilder = Json.createObjectBuilder();
        definitionBuilder.add("type", primitiveSchema.type().toTypeName());
        emitJsonProperties(primitiveSchema, context, config, definitionBuilder);
        return definitionBuilder.build();
    }
  }

  protected JsonValue writeNamedSchema(AvroNamedSchema schema, AvscWriterContext context, AvscWriterConfig config) {
    boolean seenBefore = context.schemaEncountered(schema);
    if (seenBefore) {
      return writeSchemaRef(schema, context, config);
    }
    //common parts to all named schemas
    JsonObjectBuilder definitionBuilder = Json.createObjectBuilder();
    AvroName extraAlias = emitSchemaName(schema, context, config, definitionBuilder);
    emitSchemaAliases(schema, context, config, extraAlias, definitionBuilder);
    if (schema.getDoc() != null) {
      definitionBuilder.add("doc", Json.createValue(schema.getDoc()));
    }

    AvroType type = schema.type();
    switch (type) {
      case ENUM:
        AvroEnumSchema enumSchema = (AvroEnumSchema) schema;
        definitionBuilder.add("type", "enum");
        List<String> symbols = enumSchema.getSymbols();
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for (String symbol : symbols) {
          arrayBuilder.add(symbol);
        }
        definitionBuilder.add("symbols", arrayBuilder);
        String defaultSymbol = enumSchema.getDefaultSymbol();
        if (defaultSymbol != null) {
          definitionBuilder.add("default", Json.createValue(defaultSymbol));
        }
        break;
      case FIXED:
        AvroFixedSchema fixedSchema = (AvroFixedSchema) schema;
        definitionBuilder.add("type", "fixed");
        definitionBuilder.add("size", Json.createValue(fixedSchema.getSize()));
        break;
      case RECORD:
        AvroRecordSchema recordSchema = (AvroRecordSchema) schema;
        definitionBuilder.add("type", "record"); //TODO - support error types?
        emitRecordFields(recordSchema, context, config, definitionBuilder);
        break;
      default:
        throw new IllegalStateException("not expecting " + type);
    }
    emitJsonProperties(schema, context, config, definitionBuilder);
    context.popNamingContext();
    return definitionBuilder.build();
  }

  /**
   * writes out a reference (either a full name of a simple name, if context namespace permits) to a named schema
   * @param schema a schema to write a reference to
   * @param context avsc generation context
   */
  protected JsonValue writeSchemaRef(AvroNamedSchema schema, AvscWriterContext context, AvscWriterConfig config) {
    if (config.isAlwaysEmitNamespace()) {
      //emit fullname always
      return Json.createValue(schema.getFullName());
    }
    //figure out what the context namespace is
    String contextNamespace = config.isUsePreAvro702Logic() ?
        context.getAvro702ContextNamespace() : context.getCorrectContextNamespace();
    String qualified = schema.getName().qualified(contextNamespace);
    return Json.createValue(qualified);
  }

  protected AvroName emitSchemaName(AvroNamedSchema schema, AvscWriterContext context, AvscWriterConfig config, JsonObjectBuilder output) {

    //before we get to actually writing anything we need to do some accounting of what horrible old avro would do for 702

    AvroName schemaName = schema.getName();

    //what would ancient avro do?
    String contextNamespaceAfter702;
    boolean shouldEmitNSPre702 = shouldEmitNamespace(schemaName, context.getAvro702ContextNamespace());
    if (shouldEmitNSPre702) {
      contextNamespaceAfter702 = schema.getNamespace();
    } else {
      contextNamespaceAfter702 = context.getAvro702ContextNamespace();
    }

    //what would modern avro do?
    String contextNamespaceAfter;
    boolean shouldEmitNSNormally = shouldEmitNamespace(schemaName, context.getCorrectContextNamespace());
    if (shouldEmitNSNormally) {
      contextNamespaceAfter = schema.getNamespace();
    } else {
      contextNamespaceAfter = context.getCorrectContextNamespace();
    }

    //how will Schema.parse() read the output of ancient and modern avro?
    AvroName fullnameWhenParsedUnder702 = new AvroName(schemaName.getSimpleName(), contextNamespaceAfter702);
    AvroName fullnameWhenParsed = new AvroName(schemaName.getSimpleName(), contextNamespaceAfter);

    AvroName extraAlias = null;
    if (!fullnameWhenParsed.equals(fullnameWhenParsedUnder702)) {
      if (config.isUsePreAvro702Logic()) {
        extraAlias = fullnameWhenParsed;
      } else {
        extraAlias = fullnameWhenParsedUnder702;
      }
    }

    if (config.isAlwaysEmitNamespace()) {
      if (config.isEmitNamespacesSeparately() || schemaName.getNamespace().isEmpty()) {
        //there's no way to build a fullname for something in the empty namespace
        //so for those we always need to emit an empty namespace prop.
        output.add("namespace", schemaName.getNamespace());
        output.add("name", schemaName.getSimpleName());
      } else {
        output.add("name", schemaName.getFullname());
      }
    } else {
      boolean emitNS = config.isUsePreAvro702Logic() ? shouldEmitNSPre702 : shouldEmitNSNormally;
      if (emitNS) {
        output.add("namespace", schemaName.getNamespace());
      }
      output.add("name", schemaName.getSimpleName());
    }

    context.pushNamingContext(schema, contextNamespaceAfter, contextNamespaceAfter702);

    return extraAlias;
  }

  protected void emitSchemaAliases(
      AvroNamedSchema schema,
      AvscWriterContext context,
      AvscWriterConfig config,
      AvroName extraAlias,
      JsonObjectBuilder output
  ) {
    List<AvroName> aliases = schema.getAliases();
    int numAliases = (extraAlias != null ? 1 : 0) + (aliases != null ? aliases.size() : 0);
    if (numAliases == 0) {
      return;
    }
    JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
    if (aliases != null) {
      for (AvroName alias : aliases) {
        arrayBuilder.add(alias.getFullname());
      }
    }
    if (extraAlias != null) {
      arrayBuilder.add(extraAlias.getFullname());
    }
    output.add("aliases", arrayBuilder);
  }

  protected void emitJsonProperties(
      JsonPropertiesContainer fieldOrSchema,
      AvscWriterContext context,
      AvscWriterConfig config,
      JsonObjectBuilder output
  ) {
    Set<String> propNames = fieldOrSchema.propertyNames();
    if (propNames == null || propNames.isEmpty()) {
      return;
    }
    for (String propName : propNames) {
      String json = fieldOrSchema.getPropertyAsJsonLiteral(propName);
      JsonReader reader = Json.createReader(new StringReader(json));
      JsonValue propValue = reader.readValue();
      output.add(propName, propValue);
    }
  }

  protected void emitRecordFields(AvroRecordSchema schema, AvscWriterContext context, AvscWriterConfig config, JsonObjectBuilder output) {
    JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
    List<AvroSchemaField> fields = schema.getFields();
    for (AvroSchemaField field : fields) {
      JsonObjectBuilder fieldBuilder = Json.createObjectBuilder();
      fieldBuilder.add("name", field.getName());
      if (field.hasDoc()) {
        fieldBuilder.add("doc", field.getDoc());
      }
      AvroSchema fieldSchema = field.getSchema();
      fieldBuilder.add("type", writeSchema(fieldSchema, context, config));
      if (field.hasDefaultValue()) {
        AvroLiteral defaultValue = field.getDefaultValue();
        JsonValue defaultValueLiteral = writeDefaultValue(fieldSchema, defaultValue);
        fieldBuilder.add("default", defaultValueLiteral);
      }
      //TODO - order
      //TODO - aliases
      arrayBuilder.add(fieldBuilder);
    }
    output.add("fields", arrayBuilder);
  }

  protected JsonValue writeDefaultValue(AvroSchema fieldSchema, AvroLiteral literal) {
    AvroType type = fieldSchema.type();
    String temp;
    switch (type) {
      case NULL:
        //noinspection unused (kept as a sanity check)
        AvroNullLiteral nullLiteral = (AvroNullLiteral) literal;
        return JsonValue.NULL;
      case BOOLEAN:
        AvroBooleanLiteral boolLiteral = (AvroBooleanLiteral) literal;
        return boolLiteral.getValue() ? JsonValue.TRUE : JsonValue.FALSE;
      case INT:
        AvroIntegerLiteral intLiteral = (AvroIntegerLiteral) literal;
        return Json.createValue(intLiteral.getValue());
      case LONG:
        AvroLongLiteral longLiteral = (AvroLongLiteral) literal;
        return Json.createValue(longLiteral.getValue());
      case FLOAT:
        AvroFloatLiteral floatLiteral = (AvroFloatLiteral) literal;
        return Json.createValue(floatLiteral.getValue());
      case DOUBLE:
        AvroDoubleLiteral doubleLiteral = (AvroDoubleLiteral) literal;
        return Json.createValue(doubleLiteral.getValue());
      case STRING:
        AvroStringLiteral stringLiteral = (AvroStringLiteral) literal;
        return Json.createValue(stringLiteral.getValue());
      case BYTES:
        AvroBytesLiteral bytesLiteral = (AvroBytesLiteral) literal;
        //spec  says "values for bytes and fixed fields are JSON strings, where Unicode code points
        //0-255 are mapped to unsigned 8-bit byte values 0-255", and this is how its done
        temp = new String(bytesLiteral.getValue(), StandardCharsets.ISO_8859_1);
        return Json.createValue(temp);
      case ENUM:
        AvroEnumLiteral enumLiteral = (AvroEnumLiteral) literal;
        return Json.createValue(enumLiteral.getValue());
      case FIXED:
        AvroFixedLiteral fixedLiteral = (AvroFixedLiteral) literal;
        //spec  says "values for bytes and fixed fields are JSON strings, where Unicode code points
        //0-255 are mapped to unsigned 8-bit byte values 0-255", and this is how its done
        temp = new String(fixedLiteral.getValue(), StandardCharsets.ISO_8859_1);
        return Json.createValue(temp);
      case ARRAY:
        AvroArrayLiteral arrayLiteral = (AvroArrayLiteral) literal;
        List<AvroLiteral> array = arrayLiteral.getValue();
        AvroArraySchema arraySchema = (AvroArraySchema) arrayLiteral.getSchema();
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for  (AvroLiteral element : array) {
          arrayBuilder.add(writeDefaultValue(arraySchema.getValueSchema(), element));
        }
        return arrayBuilder.build();
      default:
        throw new UnsupportedOperationException("writing default values for " + type + " not implemented yet");
    }
  }

  /**
   * checks if vanilla avro would emit a "namespace" json property given a context namespace and a schema name
   * @param name schema name, required
   * @param contextNamespace context namespace, can be null.
   * @return true if vanilla avro would emit a "namespace" json property
   */
  private boolean shouldEmitNamespace(AvroName name, String contextNamespace) {
    if (contextNamespace == null) {
      return name.getNamespace() != null && !name.getNamespace().isEmpty();
    }
    //name.namespace could be "" and sometimes need to be emitted explicitly still
    return !contextNamespace.equals(name.getNamespace());
  }
}
