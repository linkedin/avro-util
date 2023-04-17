package com.linkedin.avroutil1.model;

/**
 * The AvroSchemaDifference class is used to record the differences identified while comparing two Avro schemas. This class
 * records the code location of the difference identified in schemaA and the corrosponding location in schemaB and records the
 * summary of difference
 *  * It supports comparing schemas defined in Avro IDL (.avdl) or Avro schema (.avsc) formats.
 */
public class AvroSchemaDifference {

  /**
   * {@link CodeLocation} of the difference in schemaA
   */
  private final CodeLocation schemaALocation;

  /**
   * {@link CodeLocation} of the difference in schemaB
   */
  private final CodeLocation schemaBLocation;

  /**
   * Enum representing Avro schema difference types.
   */
  AvroSchemaDifferenceType avroSchemaDifferenceType;

  /**
   * Summary of the difference between the schemas
   */
  private final String differenceSummary;

  /**
   * Constructor for creating an AvroSchemaDifference object.
   *
   * @param schemaALocation           The {@link CodeLocation} of the difference in schemaA
   * @param schemaBLocation           The {@link CodeLocation} of the difference in schemaB
   * @param avroSchemaDifferenceType  The AvroSchemaDifferenceType representing the type of difference
   * @param differenceSummary         The summary of the difference between the schemas
   */
  public AvroSchemaDifference(CodeLocation schemaALocation,
                              CodeLocation schemaBLocation,
                              AvroSchemaDifferenceType avroSchemaDifferenceType,
                              String differenceSummary) {
    this.schemaALocation = schemaALocation;
    this.schemaBLocation = schemaBLocation;
    this.avroSchemaDifferenceType = avroSchemaDifferenceType;
    this.differenceSummary = differenceSummary;
  }

  public CodeLocation getSchemaALocation() {
    return schemaALocation;
  }

  public CodeLocation getSchemaBLocation() {
    return schemaBLocation;
  }

  public AvroSchemaDifferenceType getAvroSchemaDifferenceType() {
    return avroSchemaDifferenceType;
  }

  public String getDifferenceSummary() {
    return differenceSummary;
  }

  /**
   * Overrides the default toString() method to return a custom string representation
   * of the AvroSchemaDifference object.
   *
   * @return A string representation of the AvroSchemaDifference object
   */
  @Override
  public String toString() {
    return "Type: " + avroSchemaDifferenceType.toString() +
        ", SchemaALocation: " + ((schemaALocation != null) ? schemaALocation.toString() : "null") +
        ", SchemaBLocation: " + ((schemaBLocation != null) ? schemaBLocation.toString() : "null") +
        ", DifferenceSummary: " + differenceSummary;
  }
}
