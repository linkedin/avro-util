/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.writer.avsc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
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
import com.linkedin.avroutil1.model.AvroMapLiteral;
import com.linkedin.avroutil1.model.AvroMapSchema;
import com.linkedin.avroutil1.model.AvroName;
import com.linkedin.avroutil1.model.AvroNamedSchema;
import com.linkedin.avroutil1.model.AvroNullLiteral;
import com.linkedin.avroutil1.model.AvroPrimitiveSchema;
import com.linkedin.avroutil1.model.AvroRecordLiteral;
import com.linkedin.avroutil1.model.AvroRecordSchema;
import com.linkedin.avroutil1.model.AvroSchema;
import com.linkedin.avroutil1.model.AvroSchemaField;
import com.linkedin.avroutil1.model.AvroStringLiteral;
import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.AvroUnionSchema;
import com.linkedin.avroutil1.model.JsonPropertiesContainer;
import com.linkedin.avroutil1.model.SchemaOrRef;
import com.linkedin.avroutil1.parser.avsc.AvscUnparsedLiteral;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AvscSchemaWriter implements AvroSchemaWriter {

  private static final JsonFactory JSON_FACTORY = new JsonFactory();

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
        pathParts[i - 1] = parts[i];
      }
    }

    return Paths.get(parts[0], pathParts);
  }

  public String generateAvsc(AvroSchema schema, AvscWriterConfig config) {
    AvscWriterContext context = new AvscWriterContext();

    StringWriter stringWriter = new StringWriter();
    try (JsonGenerator generator = JSON_FACTORY.createGenerator(stringWriter)) {
      if (config.isPretty()) {
        generator.useDefaultPrettyPrinter();
      }
      writeSchema(schema, context, config, generator);
    } catch (IOException e) {
      throw new IllegalStateException("Error serializing avro schema to avsc", e);
    }

    return stringWriter.toString();
  }

  protected void writeSchema(AvroSchema schema, AvscWriterContext context, AvscWriterConfig config,
      JsonGenerator generator) throws IOException {
    AvroType type = schema.type();
    switch (type) {
      case ENUM:
      case FIXED:
      case RECORD:
        writeNamedSchema((AvroNamedSchema) schema, context, config, generator);
        break;
      case ARRAY:
        AvroArraySchema arraySchema = (AvroArraySchema) schema;
        generator.writeStartObject();
        generator.writeStringField("type", "array");
        generator.writeFieldName("items");
        writeSchema(arraySchema.getValueSchema(), context, config, generator);
        emitJsonProperties(schema, context, config, generator);
        generator.writeEndObject();
        break;
      case MAP:
        AvroMapSchema mapSchema = (AvroMapSchema) schema;
        generator.writeStartObject();
        generator.writeStringField("type", "map");
        generator.writeFieldName("values");
        writeSchema(mapSchema.getValueSchema(), context, config, generator);
        emitJsonProperties(schema, context, config, generator);
        generator.writeEndObject();
        break;
      case UNION:
        AvroUnionSchema unionSchema = (AvroUnionSchema) schema;
        generator.writeStartArray();
        for (SchemaOrRef unionBranch : unionSchema.getTypes()) {
          AvroSchema branchSchema = unionBranch.getSchema(); //will throw if unresolved ref
          writeSchema(branchSchema, context, config, generator);
        }
        generator.writeEndArray();
        break;
      default:
        AvroPrimitiveSchema primitiveSchema = (AvroPrimitiveSchema) schema;
        if (primitiveSchema.hasProperties()) {
          generator.writeStartObject();
          generator.writeStringField("type", primitiveSchema.type().toTypeName());
          emitJsonProperties(schema, context, config, generator);
          generator.writeEndObject();
        } else {
          generator.writeString(primitiveSchema.type().toTypeName());
        }
        break;
    }
  }

  protected void writeNamedSchema(AvroNamedSchema schema, AvscWriterContext context, AvscWriterConfig config,
      JsonGenerator generator) throws IOException {
    boolean seenBefore = context.schemaEncountered(schema);
    if (seenBefore) {
      writeSchemaRef(schema, context, config, generator);
      return;
    }

    // Common parts for all named schemas.
    generator.writeStartObject();
    AvroName extraAlias = emitSchemaName(schema, context, config, generator);
    emitSchemaAliases(schema, context, config, extraAlias, generator);
    if (schema.getDoc() != null) {
      generator.writeStringField("doc", schema.getDoc());
    }

    AvroType type = schema.type();
    switch (type) {
      case ENUM:
        AvroEnumSchema enumSchema = (AvroEnumSchema) schema;
        generator.writeStringField("type", "enum");
        generator.writeFieldName("symbols");
        generator.writeStartArray();
        for (String symbol : enumSchema.getSymbols()) {
          generator.writeString(symbol);
        }
        generator.writeEndArray();
        if (enumSchema.getDefaultSymbol() != null) {
          generator.writeStringField("default", enumSchema.getDefaultSymbol());
        }
        break;
      case FIXED:
        AvroFixedSchema fixedSchema = (AvroFixedSchema) schema;
        generator.writeStringField("type", "fixed");
        generator.writeNumberField("size", fixedSchema.getSize());
        break;
      case RECORD:
        AvroRecordSchema recordSchema = (AvroRecordSchema) schema;
        //TODO - support error types?
        generator.writeStringField("type", "record");
        emitRecordFields(recordSchema, context, config, generator);
        break;
      default:
        throw new IllegalStateException("not expecting " + type);
    }
    emitJsonProperties(schema, context, config, generator);
    generator.writeEndObject();

    context.popNamingContext();
  }

  /**
   * writes out a reference (either a full name of a simple name, if context namespace permits) to a named schema
   * @param schema a schema to write a reference to
   * @param context avsc generation context
   */
  protected void writeSchemaRef(AvroNamedSchema schema, AvscWriterContext context, AvscWriterConfig config,
      JsonGenerator generator) throws IOException {

    // Emit fullname always if configured to do so.
    if (config.isAlwaysEmitNamespace()) {
      generator.writeString(schema.getFullName());
    }

    // Figure out what the context namespace is
    String contextNamespace =
        config.isUsePreAvro702Logic() ? context.getAvro702ContextNamespace() : context.getCorrectContextNamespace();
    String qualified = schema.getName().qualified(contextNamespace);
    generator.writeString(qualified);
  }

  protected AvroName emitSchemaName(AvroNamedSchema schema, AvscWriterContext context, AvscWriterConfig config,
      JsonGenerator generator) throws IOException {

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
        generator.writeStringField("namespace", schemaName.getNamespace());
        generator.writeStringField("name", schemaName.getSimpleName());
      } else {
        generator.writeStringField("name", schemaName.getFullname());
      }
    } else {
      boolean emitNS = config.isUsePreAvro702Logic() ? shouldEmitNSPre702 : shouldEmitNSNormally;
      if (emitNS) {
        generator.writeStringField("namespace", schemaName.getNamespace());
      }
      generator.writeStringField("name", schemaName.getSimpleName());
    }

    context.pushNamingContext(schema, contextNamespaceAfter, contextNamespaceAfter702);

    return extraAlias;
  }

  protected void emitSchemaAliases(AvroNamedSchema schema, AvscWriterContext context, AvscWriterConfig config,
      AvroName extraAlias, JsonGenerator generator) throws IOException {
    List<AvroName> aliases = schema.getAliases();
    int numAliases = (extraAlias != null ? 1 : 0) + (aliases != null ? aliases.size() : 0);
    if (numAliases == 0) {
      return;
    }

    generator.writeFieldName("aliases");
    generator.writeStartArray();
    if (aliases != null) {
      for (AvroName alias : aliases) {
        generator.writeString(alias.getFullname());
      }
    }
    if (extraAlias != null) {
      generator.writeString(extraAlias.getFullname());
    }
    generator.writeEndArray();
  }

  protected void emitJsonProperties(JsonPropertiesContainer fieldOrSchema, AvscWriterContext context,
      AvscWriterConfig config, JsonGenerator generator) throws IOException {
    Set<String> propNames = fieldOrSchema.propertyNames();
    if (propNames == null || propNames.isEmpty()) {
      return;
    }

    for (String propName : propNames) {
      generator.writeFieldName(propName);
      generator.writeRawValue(fieldOrSchema.getPropertyAsJsonLiteral(propName));
    }
  }

  protected void emitRecordFields(AvroRecordSchema schema, AvscWriterContext context, AvscWriterConfig config,
      JsonGenerator generator) throws IOException {
    generator.writeFieldName("fields");
    generator.writeStartArray();
    for (AvroSchemaField field : schema.getFields()) {
      generator.writeStartObject();

      // Field name.
      generator.writeStringField("name", field.getName());

      // Field doc.
      if (field.hasDoc()) {
        generator.writeStringField("doc", field.getDoc());
      }

      // Field type.
      generator.writeFieldName("type");
      writeSchema(field.getSchema(), context, config, generator);

      // Field properties.
      emitJsonProperties(field.getAllProps(), context, config, generator);

      // Default value.
      if (field.hasDefaultValue()) {
        AvroLiteral defaultValue = field.getDefaultValue();
        generator.writeFieldName("default");
        writeDefaultValue(field.getSchema(), defaultValue, field, generator);
      }

      // Aliases
      // TODO - order
      if (field.aliases() != null) {
        generator.writeFieldName("aliases");
        generator.writeStartArray();
        for (String alias : field.aliases()) {
          generator.writeString(alias);
        }
        generator.writeEndArray();
      }

      generator.writeEndObject();
    }
    generator.writeEndArray();
  }

  protected void writeDefaultValue(AvroSchema schemaForLiteral, AvroLiteral literal, AvroSchemaField field,
      JsonGenerator generator) throws IOException {
    AvroType type = schemaForLiteral.type();
    String temp;
    AvroSchema valueSchema;

    // if literal is of type AvscUnparsedLiteral, throw an exception
    if (literal instanceof AvscUnparsedLiteral) {
      if (!type.equals(AvroType.UNION)) {
        throw new IllegalArgumentException(
            "Default value for field \"" + field.getName() + "\" is a malformed avro " + type.toTypeName()
                + " default value. Default value: " + ((AvscUnparsedLiteral) literal).getDefaultValueNode().toString());
      } else {
        // union defaults are commonly malformed so add what the likely issue is in the error msg
        throw new IllegalArgumentException("Default value for union field \"" + field.getName()
            + "\" should be the type of the 1st schema in the union. Default value: "
            + ((AvscUnparsedLiteral) literal).getDefaultValueNode().toString());
      }
    }

    switch (type) {
      case NULL:
        //noinspection unused (kept as a sanity check)
        AvroNullLiteral nullLiteral = (AvroNullLiteral) literal;
        generator.writeNull();
        break;
      case BOOLEAN:
        AvroBooleanLiteral boolLiteral = (AvroBooleanLiteral) literal;
        generator.writeBoolean(boolLiteral.getValue());
        break;
      case INT:
        AvroIntegerLiteral intLiteral = (AvroIntegerLiteral) literal;
        generator.writeNumber(intLiteral.getValue());
        break;
      case LONG:
        AvroLongLiteral longLiteral = (AvroLongLiteral) literal;
        generator.writeNumber(longLiteral.getValue());
        break;
      case FLOAT:
        AvroFloatLiteral floatLiteral = (AvroFloatLiteral) literal;
        generator.writeNumber(floatLiteral.getValue());
        break;
      case DOUBLE:
        AvroDoubleLiteral doubleLiteral = (AvroDoubleLiteral) literal;
        generator.writeNumber(doubleLiteral.getValue());
        break;
      case STRING:
        AvroStringLiteral stringLiteral = (AvroStringLiteral) literal;
        generator.writeString(stringLiteral.getValue());
        break;
      case BYTES:
        AvroBytesLiteral bytesLiteral = (AvroBytesLiteral) literal;
        //spec  says "values for bytes and fixed fields are JSON strings, where Unicode code points
        //0-255 are mapped to unsigned 8-bit byte values 0-255", and this is how its done
        generator.writeString(new String(bytesLiteral.getValue(), StandardCharsets.ISO_8859_1));
        break;
      case ENUM:
        AvroEnumLiteral enumLiteral = (AvroEnumLiteral) literal;
        generator.writeString(enumLiteral.getValue());
        break;
      case FIXED:
        AvroFixedLiteral fixedLiteral = (AvroFixedLiteral) literal;
        //spec  says "values for bytes and fixed fields are JSON strings, where Unicode code points
        //0-255 are mapped to unsigned 8-bit byte values 0-255", and this is how its done
        generator.writeString(new String(fixedLiteral.getValue(), StandardCharsets.ISO_8859_1));
        break;
      case ARRAY:
        AvroArrayLiteral arrayLiteral = (AvroArrayLiteral) literal;
        List<AvroLiteral> array = arrayLiteral.getValue();
        AvroArraySchema arraySchema = (AvroArraySchema) arrayLiteral.getSchema();
        valueSchema = arraySchema.getValueSchema();
        generator.writeStartArray();
        for (AvroLiteral element : array) {
          writeDefaultValue(valueSchema, element, field, generator);
        }
        generator.writeEndArray();
        break;
      case MAP:
        AvroMapLiteral mapLiteral = (AvroMapLiteral) literal;
        Map<String, AvroLiteral> map = mapLiteral.getValue();
        AvroMapSchema mapSchema = (AvroMapSchema) mapLiteral.getSchema();
        valueSchema = mapSchema.getValueSchema();
        generator.writeStartObject();
        for (Map.Entry<String, AvroLiteral> entry : map.entrySet()) {
          generator.writeFieldName(entry.getKey());
          writeDefaultValue(valueSchema, entry.getValue(), field, generator);
        }
        generator.writeEndObject();
        break;
      case UNION:
        //default values for unions must be of the 1st type in the union
        AvroUnionSchema unionSchema = (AvroUnionSchema) schemaForLiteral;
        AvroSchema firstBranchSchema = unionSchema.getTypes().get(0).getSchema();
        writeDefaultValue(firstBranchSchema, literal, field, generator);
        break;
      case RECORD:
        AvroRecordSchema recordSchema = (AvroRecordSchema) schemaForLiteral;
        generator.writeStartObject();
        Map<String, AvroLiteral> recordLiteralMap = ((AvroRecordLiteral) literal).getValue();

        for (AvroSchemaField innerField : recordSchema.getFields()) {
          generator.writeFieldName(innerField.getName());
          writeDefaultValue(innerField.getSchema(), recordLiteralMap.get(innerField.getName()), field, generator);
        }
        generator.writeEndObject();
        break;
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
