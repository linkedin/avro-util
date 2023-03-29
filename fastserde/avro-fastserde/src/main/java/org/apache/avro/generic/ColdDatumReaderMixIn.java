package org.apache.avro.generic;

import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveBooleanList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveDoubleList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveFloatList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveIntList;
import com.linkedin.avro.fastserde.coldstart.ColdPrimitiveLongList;
import java.util.Collection;
import org.apache.avro.Schema;


/**
 * An interface with default implementation in order to defeat the lack of multiple inheritance.
 */
public interface ColdDatumReaderMixIn {
  default Object newArray(Object old, int size, Schema schema, NewArrayFunction fallBackFunction) {
    switch (schema.getElementType().getType()) {
      case BOOLEAN:
        if (null == old || !(old instanceof ColdPrimitiveBooleanList)) {
          old = new ColdPrimitiveBooleanList(size);
        }
        break;
      case DOUBLE:
        if (null == old || !(old instanceof ColdPrimitiveDoubleList)) {
          old = new ColdPrimitiveDoubleList(size);
        }
        break;
      case FLOAT:
        if (null == old || !(old instanceof ColdPrimitiveFloatList)) {
          old = new ColdPrimitiveFloatList(size);
        }
        break;
      case INT:
        if (null == old || !(old instanceof ColdPrimitiveIntList)) {
          old = new ColdPrimitiveIntList(size);
        }
        break;
      case LONG:
        if (null == old || !(old instanceof ColdPrimitiveLongList)) {
          old = new ColdPrimitiveLongList(size);
        }
        break;
      default:
        return fallBackFunction.newArray(old, size, schema);
    }
    ((Collection) old).clear();
    return old;
  }

  interface NewArrayFunction {
    Object newArray(Object old, int size, Schema schema);
  }
}
