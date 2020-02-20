AvroCompatibilityHelper
=======================

This modules provides a utility class to allow performing various avro-related operations
at runtime in an avro-version-agnostic way. It is useful when, for example, one needs to
write a library that is expected to work under a wide range of avro versions at runtime.

This is done by using class `com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper`:

~~~~
System.out.println("Look ma, I can decode avro binary under " 
                   + AvroCompatibilityHelper.getRuntimeAvroVersion());
BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(inputStream ...);
SpecificDatumReader reader = new SpecificDatumReader(schema);
reader.read(null, decoder);
~~~~

The class contains method for performing operations that are impossible to do in a 
"write once, run everywhere" manner due to changes in avro's java APIs.

Project Structure and Build
===========================

* module `helper` contains the public API
* module `helper-common` contains internal utilities to the project, among them the `AvroAdapter` interface
* the `impl` folder container various `helper-impl-NN` modules, each containing code specific to
  a single major version of avro (compiled against that avro version)
* the outputs of all these modules (without any version of avro itself) are packaged into a fat jar.
  that jar is the user-consumable output of this module.