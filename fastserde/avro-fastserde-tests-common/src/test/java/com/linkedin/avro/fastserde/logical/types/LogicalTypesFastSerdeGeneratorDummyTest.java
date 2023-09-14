package com.linkedin.avro.fastserde.logical.types;

import static com.linkedin.avro.fastserde.FastSerdeTestsSupport.getCodeGenDirectory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.testng.annotations.Test;

import com.linkedin.avro.fastserde.FastGenericDeserializerGenerator;
import com.linkedin.avro.fastserde.FastGenericSerializerGenerator;
import com.linkedin.avro.fastserde.FastSpecificDeserializerGenerator;
import com.linkedin.avro.fastserde.FastSpecificSerializerGenerator;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesDefined;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesTest1;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesUndefined;
import com.linkedin.avro.fastserde.generated.avro.FastSerdeLogicalTypesWithDefaults;

/**
 * Temporary test just to generate fast-avro (de)serializer classes
 * so that we can easily see differences after adding some changes to fast-avro generators.
 */
public class LogicalTypesFastSerdeGeneratorDummyTest {

    private final File classesDir;
    private final ClassLoader classLoader;

    public LogicalTypesFastSerdeGeneratorDummyTest() {
        try {
            classesDir = getCodeGenDirectory();
            classLoader = URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()}, getClass().getClassLoader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void generateSerializersAndDeserializers() {
        SpecificRecordBase[] avroEmptyInstances = {
                new FastSerdeLogicalTypesTest1(),
                new FastSerdeLogicalTypesDefined(),
                new FastSerdeLogicalTypesUndefined(),
                new FastSerdeLogicalTypesWithDefaults()
        };

        for (SpecificRecordBase emptyInstance: avroEmptyInstances) {
            generateSerializersAndDeserializers(emptyInstance);
        }
    }

    private <T extends SpecificRecordBase> void generateSerializersAndDeserializers(T empty) {
        Schema schema = empty.getSchema();
        SpecificData specificData = empty.getSpecificData();
        GenericData genericData = toGenericModelData(specificData);

        new FastGenericSerializerGenerator<GenericData.Record>(
                schema, classesDir, classLoader, null)
                .generateSerializer();
        new FastSpecificSerializerGenerator<T>(
                schema, classesDir, classLoader, null)
                .generateSerializer();
        new FastGenericDeserializerGenerator<GenericData.Record>(
                schema, schema, classesDir, classLoader, null)
                .generateDeserializer();
        new FastSpecificDeserializerGenerator<T>(
                schema, schema, classesDir, classLoader, null)
                .generateDeserializer();
    }

    private GenericData toGenericModelData(SpecificData fromSpecificData) {
        GenericData genericData = new GenericData();
        Optional.ofNullable(fromSpecificData.getConversions())
                .orElse(Collections.emptyList())
                .forEach(genericData::addLogicalTypeConversion);

        return genericData;
    }
}
