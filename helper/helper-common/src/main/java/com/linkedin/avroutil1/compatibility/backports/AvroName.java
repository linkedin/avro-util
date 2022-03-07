/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.backports;

import com.linkedin.avroutil1.compatibility.HelperConsts;
import com.linkedin.avroutil1.compatibility.JsonGeneratorWrapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

public class AvroName {
    public static final Comparator<AvroName> BY_FULLNAME = Comparator.comparing(AvroName::getFull);

    private final String name;
    private final String space;
    private final String full;

    public static AvroName of(Schema schema) {
        Schema.Type type = schema.getType();
        if (!HelperConsts.NAMED_TYPES.contains(type)) {
            throw new IllegalArgumentException("dont know how to build an AvroName out of " + type + " " + schema);
        }
        return new AvroName(schema.getFullName(), null); //will handle namespace parsing internally
    }

    public AvroName(String name, String space) {
        if (name == null) { // anonymous
            this.name = this.space = this.full = null;
            return;
        }
        int lastDot = name.lastIndexOf('.');
        if (lastDot < 0) { // unqualified name
            this.name = validateName(name);
        } else { // qualified name
            space = name.substring(0, lastDot); // get space from name
            this.name = validateName(name.substring(lastDot + 1, name.length()));
        }
        if ("".equals(space)) {
            space = null;
        }
        this.space = space;
        this.full = (this.space == null) ? this.name : this.space + "." + this.name;
    }

    public String getName() {
        return name;
    }

    public String getSpace() {
        return space;
    }

    public String getFull() {
        return full;
    }

    public boolean isAnonymous() {
        return name == null;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof AvroName)) {
            return false;
        }
        AvroName that = (AvroName) o;
        return Objects.equals(full, that.full);
    }

    @Override
    public int hashCode() {
        return full == null ? 0 : full.hashCode();
    }

    @Override
    public String toString() {
        return full;
    }

    /**
     * writes this name (as a name prop and optionally a namespace prop) to an underlying json genrator
     * @param names the set of "known fullnames" and current "inherited" namespace used by the current avsc
     *              generation operation
     * @param preAvro702 true to emit the same output as avro 1.4 would (before avro-702 was fixed in 1.5+)
     * @param namespaceWhenParsed the context namespace at this point under correct output
     * @param namespaceWhen702 the context namespace at this point under pre-avro-702 output
     * @param gen json output to write to
     * @return information about what was actually done and how ancient vs modern avro would emit this named type
     * @throws IOException
     */
    public Avro702Data writeName(
        AvroNames names,
        boolean preAvro702,
        String namespaceWhenParsed,
        String namespaceWhen702,
        JsonGeneratorWrapper<?> gen
    ) throws IOException {
        //always emit a name (if we have one?)
        if (name != null) {
            gen.writeStringField("name", name);
        }

        String cleanNamespace = space == null ? "" : space;

        //what would ancient avro do?
        String contextNamespaceAfter702;
        boolean shouldEmitNSPre702 = shouldEmitNamespace(names.badSpace());
        if (shouldEmitNSPre702) {
            contextNamespaceAfter702 = cleanNamespace;
        } else {
            contextNamespaceAfter702 = namespaceWhen702;
        }

        //what would modern avro do?
        String contextNamespaceAfter;
        boolean shouldEmitNSNormally = shouldEmitNamespace(names.correctSpace());
        if (shouldEmitNSNormally) {
            contextNamespaceAfter = cleanNamespace;
        } else {
            contextNamespaceAfter = namespaceWhenParsed;
        }

        //what should we do?
        boolean emitNS = preAvro702 ? shouldEmitNSPre702 : shouldEmitNSNormally;
        if (emitNS) {
            gen.writeStringField("namespace", cleanNamespace);
        }

        //how will Schema.parse() read the output of ancient and modern avro?
        AvroName fullnameWhenParsed = new AvroName(this.name, contextNamespaceAfter);
        AvroName fullnameWhenParsedUnder702 = new AvroName(this.name, contextNamespaceAfter702);

        //how will Schema.parse() read our actual output?
        AvroName asWritten = preAvro702 ? fullnameWhenParsedUnder702 : fullnameWhenParsed;

        List<AvroName> aliases = new ArrayList<>(0);
        if (!fullnameWhenParsed.equals(fullnameWhenParsedUnder702)) {
            if (preAvro702) {
                aliases.add(fullnameWhenParsed);
            } else {
                aliases.add(fullnameWhenParsedUnder702);
            }
        }

        return new Avro702Data(
            this,
            asWritten,
            aliases,
            contextNamespaceAfter,
            contextNamespaceAfter702
        );
    }

    public String getQualified(String defaultSpace) {
        return (space == null || space.equals(defaultSpace)) ? name : full;
    }

    private boolean shouldEmitNamespace(String contextNamespace) {
        if (space != null) { //do we have a namespace?
            //is our namespace different than the current context one?
            //(also works when the context namespace is null)
            return !space.equals(contextNamespace);
        } else {
            //we're in the default namespace. if context is something else
            //we will need to emit (an empty) namespace
            return contextNamespace != null;
        }
    }

    private static String validateName(String name) {
        if (name == null) {
            throw new SchemaParseException("Null name");
        }
        int length = name.length();
        if (length == 0) {
            throw new SchemaParseException("Empty name");
        }
        char first = name.charAt(0);
        if (!(Character.isLetter(first) || first == '_')) {
            throw new SchemaParseException("Illegal initial character: " + name);
        }
        for (int i = 1; i < length; i++) {
            char c = name.charAt(i);
            if (!(Character.isLetterOrDigit(c) || c == '_')) {
                throw new SchemaParseException("Illegal character in: " + name);
            }
        }
        return name;
    }
}
