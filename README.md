Avro-Util
=========
[![Build Status](https://travis-ci.org/linkedin/avro-util.svg?branch=master)](https://travis-ci.org/linkedin/avro-util)
[![Download](https://api.bintray.com/packages/linkedin/maven/avro-util/images/download.svg)](https://bintray.com/linkedin/maven/avro-util/_latestVersion)

A collection of utilitiesto allow java projects to better work with avro.

### Background ###

Apache Avro is a widely used serialization format.

Unfortunately it is not always possible to write java code that "just works" 
across multiple versions of avro, as there have been breaking changes to both 
the API and wire format. This project provides utility code to enable java 
developers to write code that is compatible across a wide range of avro versions 

Furthermore, this project enables Avro-based projects to achieve better 
performance.

### Modules ###

The following modules are available in this project.

## avro-migration-helper ##

This module provides utility functions which, when coded against, ensure 
compatibility with every Avro supported version. This is achieved by offering 
a single API which, under the hood, delegates to the Avro version which is 
detected at runtime.

## avro-codegen ##

This module provides code generation capabilities which allows an auto-generated
SpecificRecord class to work with all supported versions of Avro.

## avro-fastserde ##

This is a fork of https://github.com/RTBHOUSE/avro-fastserde

In a nut shell, avro-fastserde enables faster Avro de/serialization by doing
runtime code-generation to provide a faster decoder/encoder implementation.

As part of this fork, there are compatibility improvements to ensure that the
decoding semantics are the same as that of regular Avro (in particular around
the handling of String types in modern Avro). Moreover, this fork enables 
partial compatibility with older versions of Avro. Finally, this fork provides
supports for object re-use, a standard Avro deserialization feature, as well
as some other garbage-collection optimizations which have proven useful at
LinkedIn.

### Supported versions of Avro ###

| Version  | Serialization | Deserialization | Fast Serialization | Fast Deserialization |
| -------- | ------------- | --------------- | ------------------ | -------------------- |
| Avro 1.4 |      Yes      |      Yes        |      Yes            |      Yes             |
| Avro 1.5 |      ???      |      ???        |      No            |      No              |
| Avro 1.6 |      ???      |      ???        |      No            |      No              |
| Avro 1.7 |      Yes      |      Yes        |      Yes           |      Yes             |
| Avro 1.8 |      Yes      |      Yes        |      Yes           |      Yes             |
