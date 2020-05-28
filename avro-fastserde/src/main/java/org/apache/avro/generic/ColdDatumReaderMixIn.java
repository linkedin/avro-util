package org.apache.avro.generic;

import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveFloatList;
import java.util.Collection;
import org.apache.avro.Schema;


/**
 * An interface with default implementation in order to defeat the lack of multiple inheritance.
 */
public interface ColdDatumReaderMixIn {
  default Object newArray(Object old, int size, Schema schema, NewArrayFunction fallBackFunction) {
    switch (schema.getElementType().getType()) {
      case FLOAT:
        if (null == old || !(old instanceof ColdPrimitiveFloatList)) {
          return new ColdPrimitiveFloatList(size);
        }
        ((Collection) old).clear();
        return old;
      // TODO: Add more cases when we support more primitive array types
      default:
        return fallBackFunction.newArray(old, size, schema);
    }
  }

  interface NewArrayFunction {
    Object newArray(Object old, int size, Schema schema);
  }
}
