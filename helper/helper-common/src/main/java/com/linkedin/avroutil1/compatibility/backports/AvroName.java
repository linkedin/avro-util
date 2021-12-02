/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.backports;

import com.linkedin.avroutil1.compatibility.JsonGeneratorWrapper;
import org.apache.avro.SchemaParseException;

import java.io.IOException;
import java.util.Objects;

public class AvroName {
    private final String name;
    private final String space;
    private final String full;

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

    public void writeName(AvroNames names, JsonGeneratorWrapper<?> gen) throws IOException {
        if (name != null) {
            gen.writeStringField("name", name);
        }
        if (space != null) {
            if (!space.equals(names.space())) {
                gen.writeStringField("namespace", space);
            }
        } else if (names.space() != null) { // null within non-null
            gen.writeStringField("namespace", "");
        }
    }

    public String getQualified(String defaultSpace) {
        return (space == null || space.equals(defaultSpace)) ? name : full;
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
