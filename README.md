Avro-Util
=========
[![Github Build Status](https://github.com/linkedin/avro-util/workflows/push%20flow/badge.svg)](https://github.com/linkedin/avro-util/actions/workflows/push.yaml)
![Latest Release](https://img.shields.io/badge/dynamic/json?color=blue&label=latest&query=%24.results%5B0%5D.version&url=https%3A%2F%2Flinkedin.jfrog.io%2Fartifactory%2Fapi%2Fsearch%2Fversions%3Fg%3Dcom.linkedin.avroutil1%26a%3Dhelper-all%26repos%3Davro-util)

A collection of utilities and libraries to allow java projects to better work with avro.

### 0.4.37 maintenance branch ###

This branch backports the security fixes from #604 and #606 onto 0.4.37:
schema-documentation escaping in Java code generation and class validation in
specific fast-serde code generation. It also includes the negative-integer
regression coverage from #602.

It intentionally excludes #595, preserving 0.4.37's range-checked `long` to `int`
conversion, including rejection of values outside the signed 32-bit range.
Consumers already on 0.4.38 or later should use 0.4.40 or a newer appropriate
release rather than this compatibility branch. Maintenance artifacts must be
published under a new version; the existing 0.4.37 release must not be replaced.

### How To Use ###

Artifacts are published to [artifactory](https://linkedin.jfrog.io/artifactory/avro-util/).
To use them (say in a gradle build) you'd need to add that repository to your build.gradle, 
and then add a dependency on the module(s) you wish to use:

```gradle
repositories {
   maven {
      url  "https://linkedin.jfrog.io/artifactory/avro-util/"
   }
}
...
dependencies {
   compile "com.linkedin.avroutil1:helper-all:<latest version>"
}
```

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

## helper ##

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

The helper module supports avro 1.4 - 1.11 inclusive, fastserde included.