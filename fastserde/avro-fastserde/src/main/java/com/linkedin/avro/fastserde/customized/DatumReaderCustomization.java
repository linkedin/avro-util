package com.linkedin.avro.fastserde.customized;

import java.util.HashMap;
import java.util.Map;


/**
 * Customization for de-serialization.
 * This class is expandable.
 */
public class DatumReaderCustomization {

  public interface NewMapOverrideFunction {
    Object apply(Object old, int size);
  }

  private NewMapOverrideFunction newMapOverrideFunc;


  private DatumReaderCustomization(Builder builder) {
    this.newMapOverrideFunc = builder.newMapOverrideFunc;
  }

  public NewMapOverrideFunction getNewMapOverrideFunc() {
    return newMapOverrideFunc;
  }

  public static class Builder {
    private NewMapOverrideFunction newMapOverrideFunc;

    public Builder() {}

    public Builder setNewMapOverrideFunc(NewMapOverrideFunction newMapOverrideFunc) {
      this.newMapOverrideFunc = newMapOverrideFunc;
      return this;
    }

    public DatumReaderCustomization build() {
      return new DatumReaderCustomization(this);
    }
  }

  public static final DatumReaderCustomization DEFAULT_DATUM_READER_CUSTOMIZATION = new DatumReaderCustomization.Builder()
      .setNewMapOverrideFunc((old, size) -> {
        Map retMap = null;
        if (old instanceof Map) {
          retMap = (Map)old;
        }
        if (retMap != null) {
          retMap.clear();
        } else {
          /**
           * Pure integer arithmetic equivalent of (int) Math.ceil(expectedSize / 0.75).
           * The default load factor of HashMap is 0.75 and HashMap internally ensures size is always a power of two.
           */
          retMap = new HashMap<>((size * 4 + 2) / 3);
        }
        return retMap;
      })
      .build();
}
