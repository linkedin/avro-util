# Spotbugs-Plugin

this module holds a plugin containing [spotbugs](https://spotbugs.github.io/) rules for finding usage of incompatible avro APIs in java code.

## how to use

### gradle projects

(for a complete example see te spotbugs demo)

1st make sure your project is using the [spotbugs gradle plugin](https://plugins.gradle.org/plugin/com.github.spotbugs):
```
plugins {
  id "com.github.spotbugs" version "<latest spotbugs-gradle-plugin>"
}
```
then configure both the version of spotbugs itself and add this plugin via dependencies:
```
dependencies {
  spotbugs "com.github.spotbugs:spotbugs:<latest spotbugs>"
  spotbugsPlugins "com.linkedin.avroutil1.spotbugs-plugin:<latest avro-util>"        
}
```
for further details on how to customize includes/excludes and reporting, see [spotbugs-gradle-plugin docs](https://github.com/spotbugs/spotbugs-gradle-plugin)

### command line

(for complete docs see [relevant section of the spotbugs docs](https://spotbugs.readthedocs.io/en/stable/running.html))

```
java -jar spotbugs.jar -pluginList spotbugs-plugin.jar ...
```
## table of incompatible APIs and their helper replacements

| code | rule | incompatible code | compatible replacement |
|------|------|-------------------|------------------------|
| BDI  | BinaryDecoder instantiation | ```new BinaryDecoder(...)```  | ```AvroCompatibilityHelper.newBinaryDecoder(...)``` |
| BEI  | BinaryEncoder instantiation | ```new BinaryEncoder(...)```  | ```AvroCompatibilityHelper.newBinaryEncoder(...)``` |
| ESI  | EnumSymbol instantiation    | ```new EnumSymbol(...)```     | ```AvroCompatibilityHelper.newEnumSymbol(...)``` |
| FDVA | Field default value access  | ```field.defaultValue()```    | ```AvroCompatibilityHelper.fieldHasDefault(...)``` and then ```AvroCompatibilityHelper.get<Generic\|Specific>DefaultValue(...)``` |
|      |                             | ```field.defaultVal()```      | ```AvroCompatibilityHelper.fieldHasDefault(...)``` and then ```AvroCompatibilityHelper.get<Generic\|Specific>DefaultValue(...)``` |
| FI   | Fixed Instantiation         | ```new Fixed(...)```          | ```AvroCompatibilityHelper.newFixed(...)``` |
| GRB  | GenericRecordBuilder usage  | ```new GenericRecordBuilder()``` | use ```GenericData.Record``` directly |
| IOGR | instanceof GenericRecord    | ```x instanceof GenericRecord``` | ```AvroCompatibilityHelper.isGenericRecord(...)``` |
| JDI  | JsonDecoder instantiation   | ```new JsonDecoder(...)``` | ```AvroCompatibilityHelper.new<Compatible>JsonDecoder(...)``` |
| JDFI |                             | ```DecoderFactory.jsonDecoder(...)``` | ```AvroCompatibilityHelper.new<Compatible>JsonDecoder(...)``` |
| JEI  | JsonEncoder instantiation   | ```new JsonEncoder(...)``` | ```AvroCompatibilityHelper.newJsonEncoder(...)``` |
| OSCI | SchemaConstructable usage   | ```implements SchemaConstructable``` | avoid |
|      | SchemaConstructable instantiation | part of decoding | ```AvroCompatibilityHelper.newInstance(...)``` |
| PA   | Schema/Field prop access    | ```x.getJsonProp()``` | ```AvroCompatibilityHelper.get<Field\|Schema>PropAsJsonString(...)``` |
|      |                             | ```x.getObjectProp()``` | ```AvroCompatibilityHelper.get<Field\|Schema>PropAsJsonString(...)``` |
|      |                             | ```x.props()``` | ```AvroCompatibilityHelper.get<Field\|Schema>PropAsJsonString(...)``` |
|      |                             | ```x.getJsonProps()``` | ```AvroCompatibilityHelper.get<Field\|Schema>PropAsJsonString(...)``` |
|      |                             | ```x.getObjectProps()``` | ```AvroCompatibilityHelper.get<Field\|Schema>PropAsJsonString(...)``` |
|      |                             | ```x.addProp(<JsonNode\|Object>)``` | ```AvroCompatibilityHelper.cloneSchema(...) or AvroCompatibilityHelper.createSchemaField(...)``` |
|      |                             | ```x.addAllProps(...)``` | ```AvroCompatibilityHelper.cloneSchema(...) or AvroCompatibilityHelper.createSchemaField(...)``` |
| SFI  | Schema.Field instantiation  | ```new Schema.Field(...)``` | ```AvroCompatibilityHelper.createSchemaField(...)``` |
| SDNI | SpecificData.newInstance()  | ```SpecificData.newInstance(...)``` | ```AvroCompatibilityHelper.newInstance(...)``` |

## incompatible APIs in detail

TBD
