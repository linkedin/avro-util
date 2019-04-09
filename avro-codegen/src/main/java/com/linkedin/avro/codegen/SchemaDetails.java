package com.linkedin.avro.codegen;

import org.apache.avro.Schema;

import java.io.File;

class SchemaDetails {
    private Schema schema;
    private SchemaSource source;

    //fields valid for schemas sourced from files
    private File location;
    private boolean topLevel;

    //fields valid for schemas sources from the classpath
    private boolean reversed;

    static SchemaDetails fromFile(Schema schema, File location, boolean topLevel) {
        return new SchemaDetails(schema, SchemaSource.FILE, location, topLevel, false);
    }

    static SchemaDetails fromClasspath(Schema schema, boolean reversed) {
        return new SchemaDetails(schema, SchemaSource.CLASSPATH, null, false, reversed);
    }

    private SchemaDetails(Schema schema, SchemaSource source, File location, boolean topLevel, boolean reversed) {
        this.schema = schema;
        this.source = source;
        this.location = location;
        this.topLevel = topLevel;
        this.reversed = reversed;
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaSource getSource() {
        return source;
    }

    public File getLocation() {
        return location;
    }

    public boolean isTopLevel() {
        return topLevel;
    }

    public boolean isReversed() {
        return reversed;
    }
}
