Avro-Util
=========

A collection of utilities and helper code to allow java projects to work
across a wide version of avro versions

### Background ###

Apache Avro is a widely used serialization format.

Unfortunately it is not always possible to write java code that "just works" 
across multiple versions of avro, as there have been breaking changes to both 
the API and wire format.

This project provides utility code to enable java developers to write code 
that is compatible across a wide range of avro versions 