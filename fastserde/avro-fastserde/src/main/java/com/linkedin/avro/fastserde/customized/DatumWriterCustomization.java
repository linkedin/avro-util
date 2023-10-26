package com.linkedin.avro.fastserde.customized;

/**
 * Customization for serialization.
 * This class is expandable.
 */
public class DatumWriterCustomization {
  private CheckMapTypeFunction checkMapTypeFunction;

  private DatumWriterCustomization(Builder builder) {
    this.checkMapTypeFunction = builder.checkMapTypeFunction;
  }

  public CheckMapTypeFunction getCheckMapTypeFunction() {
    return checkMapTypeFunction;
  }

  public interface CheckMapTypeFunction {
    void apply(Object o);
  }

  public static class Builder {
    private CheckMapTypeFunction checkMapTypeFunction;

    public Builder() {}

    public Builder setCheckMapTypeFunction(CheckMapTypeFunction checkMapTypeFunction) {
      this.checkMapTypeFunction = checkMapTypeFunction;
      return this;
    }

    public DatumWriterCustomization build() {
      return new DatumWriterCustomization(this);
    }
  }

  public static final DatumWriterCustomization DEFAULT_DATUM_WRITER_CUSTOMIZATION = new DatumWriterCustomization.Builder()
      .setCheckMapTypeFunction( o -> {})
      .build();
}
