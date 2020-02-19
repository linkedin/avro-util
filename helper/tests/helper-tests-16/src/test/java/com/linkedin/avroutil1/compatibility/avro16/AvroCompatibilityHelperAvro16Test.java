package com.linkedin.avroutil1.compatibility.avro16;

import com.linkedin.avroutil1.Pojo;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperAvro16Test {

  @Test
  public void testSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Avro16SchemaConstructable.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Avro16SchemaConstructable);
    Avro16SchemaConstructable constructable = (Avro16SchemaConstructable)instance;
    Assert.assertEquals(constructable.getSchema(), schema);
  }

  @Test
  public void testNonSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Pojo.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Pojo);
  }
}
