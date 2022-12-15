/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FieldAccessorTest {

  @Test
  public void testStringRepForJavaTypes() throws Exception {

    //simple props
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("stringField")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("utf8Field")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("charSequenceField")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setString")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setUtf8")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setCharSequence")), StringRepresentation.Utf8);

    //simple list props
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("stringListField")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("utf8ListField")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("charSequenceListField")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setStringList")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setUtf8List")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setCharSequenceList")), StringRepresentation.Utf8);

    //simple map props
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("strStrMapField")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("utfUtfMapField")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("csCsMapField")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("strIntMapField")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("utfIntMapField")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForField(getFieldByName("csIntMapField")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setStrStrMap")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setUtfUtfMap")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setCsCsMap")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setStrIntMap")), StringRepresentation.String);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setUtfIntMap")), StringRepresentation.Utf8);
    Assert.assertEquals(FieldAccessor.stringRepForSetter(getMethodByName("setCsIntMap")), StringRepresentation.Utf8);
    int h = 7;
  }

  //these exist just for their signature

  public String stringField;
  public Utf8 utf8Field;
  public CharSequence charSequenceField;
  public void setString(String value) {}
  public void setUtf8(Utf8 value) {}
  public void setCharSequence(CharSequence value) {}

  public List<String> stringListField;
  public List<Utf8> utf8ListField;
  public List<CharSequence> charSequenceListField;
  public void setStringList(List<String> value) {}
  public void setUtf8List(List<Utf8> value) {}
  public void setCharSequenceList(List<CharSequence> value) {}

  public Map<String, String> strStrMapField;
  public Map<Utf8, Utf8> utfUtfMapField;
  public Map<CharSequence, CharSequence> csCsMapField;
  public Map<String, Integer> strIntMapField;
  public Map<Utf8, Integer> utfIntMapField;
  public Map<CharSequence, Integer> csIntMapField;
  public void setStrStrMap(Map<String, String> value) {}
  public void setUtfUtfMap(Map<Utf8, Utf8> value) {}
  public void setCsCsMap(Map<CharSequence, CharSequence> value) {}
  public void setStrIntMap(Map<String, Integer> value) {}
  public void setUtfIntMap(Map<Utf8, Integer> value) {}
  public void setCsIntMap(Map<CharSequence, Integer> value) {}

  private Method getMethodByName(String name) {
    Method[] methods = getClass().getMethods();
    Method match = null;
    for (Method candidate : methods) {
      if (name.equals(candidate.getName())) {
        if (match == null) {
          match = candidate;
        } else {
          throw new IllegalStateException("2+ methods called " + name + ": " + match + " and " + candidate);
        }
      }
    }
    if (match == null) {
      throw new IllegalStateException("cant find method " + name);
    }
    return match;
  }

  private Field getFieldByName(String name) {
    Field[] fields = getClass().getFields();
    Field match = null;
    for (Field candidate : fields) {
      if (name.equals(candidate.getName())) {
        if (match == null) {
          match = candidate;
        } else {
          throw new IllegalStateException("2+ fields called " + name + ": " + match + " and " + candidate);
        }
      }
    }
    if (match == null) {
      throw new IllegalStateException("cant find field " + name);
    }
    return match;
  }
}
