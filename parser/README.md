Parser
=========

This module contains a parser for avro schemas out of avsc json 

Major features planned:

* all parsed schemas and their components retain their "locations"
  (line/column/character) in the original source material
* detailed, helpful error messages that pin-point the exact location
  of issues in schemas
* classification of schema issues into types and severity
* the ability to reuse ("import") schemas defined in either other *.avsc files in the same
  "parsing unit", allowing for schema reuse inside and between large projects

Detected schema issues planned:

* mismatch between field types and default value types
* duplicate schemas in the same "parsing unit"/project
* "forward references"
* reuse/import of "nested" (not top-level) schemas
* use of "full" names (names containing '.')
* use of namespace in combination with a "full" name (where it's ignored)
* mismatch between a schema's namespace and its path under
  a project's root folder
* schemas with no (default) namespace
* multiple fields with the same case-insensitive name
* enum types without a defined default value
  
instructions and examples TBD     