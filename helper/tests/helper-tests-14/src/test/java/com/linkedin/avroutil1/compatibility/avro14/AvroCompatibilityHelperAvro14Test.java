package com.linkedin.avroutil1.compatibility.avro14;

import com.linkedin.avroutil1.Pojo;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroCompatibilityHelperAvro14Test {

  @Test
  public void testSchemaConstructableNewInstance() {
    Schema schema = Mockito.mock(Schema.class);
    Object instance = AvroCompatibilityHelper.newInstance(Avro14SchemaConstructable.class, schema);
    Assert.assertNotNull(instance);
    Assert.assertTrue(instance instanceof  Avro14SchemaConstructable);
    Avro14SchemaConstructable constructable = (Avro14SchemaConstructable)instance;
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
