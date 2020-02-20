Avro Codegen
=========

This module contains utility classes for generating java classes out of avro schema (*.avsc) files 

Major features include:

* the ability to reuse ("import") schemas defined in either other *.avsc files in the same
  code generation scope of schemas defined by classes on the classpath when generating code, 
  allowing for schema reuse inside and between large projects
* the ability to generate java code for specific records that is runtime-compatible with a
  wide range of avro versions, allowing to reuse the same libraries of schemas between 
  projects that use different versions of avro
  
instructions and examples TBD     