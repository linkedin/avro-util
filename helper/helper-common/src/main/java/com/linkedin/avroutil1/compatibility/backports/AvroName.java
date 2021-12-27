/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.backports;

import com.linkedin.avroutil1.compatibility.HelperConsts;
import com.linkedin.avroutil1.compatibility.JsonGeneratorWrapper;
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
     * @param gen json output to write to
     * @return an alternate name to this, if names and alternativeNames would result in different output from
     * this object. more specifically - if the "effective full name" (simple name + either explicitly printed
     * or inherited namespace) for this AvroName would be different under alternativeNames, the fullname
     * under alternativeNames is returned.
     * if the result of this method would have been the exact same under alternativeNames returns null
     * @throws IOException
     */
    public AvroName writeName(AvroNames names, boolean preAvro702, JsonGeneratorWrapper<?> gen) throws IOException {
        //always emit a name (if we have one?)
        if (name != null) {
            gen.writeStringField("name", name);
        }
        boolean shouldEmitNSPre702 = shouldEmitNamespace(names.badSpace());
        boolean shouldEmitNSNormally = shouldEmitNamespace(names.correctSpace());
        boolean emitNS = preAvro702 ? shouldEmitNSPre702 : shouldEmitNSNormally;
        if (emitNS) {
            String toEmit = space == null ? "" : space;
            gen.writeStringField("namespace", toEmit);
        }
        //even under 702, if we emit a NS explicitly we get the correct fullname (==this)
        //otherwise we are creating (effectively) a fullname that is our simpleName combined
        //with the (correct!) current "context" namespace
        AvroName effectiveNameUnder702 = shouldEmitNSPre702 ? this : new AvroName(this.name, names.correctSpace());
        //if our (effective) name under 702 is different than our correct name we need to return an alias
        if (!effectiveNameUnder702.equals(this)) {
            //if we are generating pre-702 output it means we just emitted effectiveNameUnder702 and need to return
            //ourselves (the correct fullname) as an alias. otherwise the opposite
            return preAvro702 ? this : effectiveNameUnder702;
        }
        return null;
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
