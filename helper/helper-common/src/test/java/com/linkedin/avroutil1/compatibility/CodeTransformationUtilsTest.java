/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import org.testng.Assert;
import org.testng.annotations.Test;


public class CodeTransformationUtilsTest {

    @Test
    public void testFindEndOfBlock() {
        // Test basic code block
        String code1 = "public void method() { int x = 1; return; }";
        int result1 = CodeTransformationUtils.findEndOfBlock(code1, code1.indexOf('{'));
        Assert.assertEquals(result1, code1.length());

        // Test nested blocks
        String code2 = "public void method() { if (true) { int x = 1; } return; }";
        int result2 = CodeTransformationUtils.findEndOfBlock(code2, code2.indexOf('{'));
        Assert.assertEquals(result2, code2.length());

        // Test multiple nested blocks
        String code3 = "public void method() { if (true) { while(x < 10) { x++; } } return; }";
        int result3 = CodeTransformationUtils.findEndOfBlock(code3, code3.indexOf('{'));
        Assert.assertEquals(result3, code3.length());

        // Test invalid start position
        int result4 = CodeTransformationUtils.findEndOfBlock("public void method() {}", -1);
        Assert.assertEquals(result4, -1);

        // Test no opening brace
        int result5 = CodeTransformationUtils.findEndOfBlock("public void method()", 0);
        Assert.assertEquals(result5, -1);

        // Test unclosed block
        String code6 = "public void method() { if (true) {";
        int result6 = CodeTransformationUtils.findEndOfBlock(code6, code6.indexOf('{'));
        Assert.assertEquals(result6, -1);

        // Test second block in code
        String code7 = "public void method1() {} public void method2() { return; }";
        int firstBraceIndex = code7.indexOf('{');
        int secondBraceIndex = code7.indexOf('{', firstBraceIndex + 1);
        int result7 = CodeTransformationUtils.findEndOfBlock(code7, secondBraceIndex);
        Assert.assertEquals(result7, code7.length());
    }

    @Test
    public void testGenerateNumericConversionJavadoc() {
        // Test Integer to Long conversion (regular setter)
        StringBuilder result1 = CodeTransformationUtils.generateNumericConversionJavadoc(
                "count", "java.lang.Integer", "java.lang.Long", false);
        Assert.assertTrue(result1.toString().contains("Sets count to the specified value"));
        Assert.assertTrue(result1.toString().contains("Accepts an Integer value and converts it to Long"));
        Assert.assertTrue(result1.toString().contains("@param value The Integer value to set"));
        Assert.assertFalse(result1.toString().contains("@return"));

        // Test long to int conversion (builder method)
        StringBuilder result2 = CodeTransformationUtils.generateNumericConversionJavadoc(
                "age", "long", "int", true);
        Assert.assertTrue(result2.toString().contains("Sets age to the specified value"));
        Assert.assertTrue(result2.toString().contains("Accepts a long value and converts it to int with bounds checking"));
        Assert.assertTrue(result2.toString().contains("@param value The long value to set"));
        Assert.assertTrue(result2.toString().contains("@return This builder"));
        Assert.assertTrue(result2.toString().contains("@throws org.apache.avro.AvroRuntimeException"));
    }

    @Test
    public void testGenerateNumericMethodSignature() {
        // Test regular setter signature
        StringBuilder sig1 = CodeTransformationUtils.generateNumericMethodSignature(
                "setValue", "long", "value", "void");
        Assert.assertEquals(sig1.toString(), "    public void setValue(long value) {\n");

        // Test builder method signature
        StringBuilder sig2 = CodeTransformationUtils.generateNumericMethodSignature(
                "withCount", "java.lang.Integer", "value", "Builder");
        Assert.assertEquals(sig2.toString(), "    public Builder withCount(java.lang.Integer value) {\n");
    }

    @Test
    public void testGenerateNumericConversionCode() {
        // Test long to int conversion (regular setter)
        StringBuilder code1 = CodeTransformationUtils.generateNumericConversionCode(
                "count", "long", "int", false, null);
        Assert.assertTrue(code1.toString().contains("if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE)"));
        Assert.assertTrue(code1.toString().contains("this.count = (int) value"));
        Assert.assertTrue(code1.toString().contains("throw new org.apache.avro.AvroRuntimeException"));

        // Test int to long conversion (builder method)
        StringBuilder code2 = CodeTransformationUtils.generateNumericConversionCode(
                "size", "int", "long", true, "withSize");
        Assert.assertTrue(code2.toString().contains("return withSize((long) value)"));

        // Test Long to Integer conversion (regular setter with null handling)
        StringBuilder code3 = CodeTransformationUtils.generateNumericConversionCode(
                "count", "java.lang.Long", "java.lang.Integer", false, null);
        Assert.assertTrue(code3.toString().contains("if (value == null)"));
        Assert.assertTrue(code3.toString().contains("this.count = null"));
        Assert.assertTrue(code3.toString().contains("this.count = value.intValue()"));

        // Test Integer to Long conversion (builder method with null handling)
        StringBuilder code4 = CodeTransformationUtils.generateNumericConversionCode(
                "size", "java.lang.Integer", "java.lang.Long", true, "withSize");
        Assert.assertTrue(code4.toString().contains("if (value == null)"));
        Assert.assertTrue(code4.toString().contains("return withSize((java.lang.Long) null)"));
        Assert.assertTrue(code4.toString().contains("return withSize(value.longValue())"));
    }

    @Test
    public void testDetermineOverloadSignature() {
        // Test primitive types
        Assert.assertEquals(CodeTransformationUtils.determineOverloadSignature("int", "setValue"), 
                "public void setValue(long ");
        Assert.assertEquals(CodeTransformationUtils.determineOverloadSignature("long", "setValue"), 
                "public void setValue(int ");
        
        // Test wrapper types
        Assert.assertEquals(CodeTransformationUtils.determineOverloadSignature("java.lang.Integer", "setValue"), 
                "public void setValue(java.lang.Long ");
        Assert.assertEquals(CodeTransformationUtils.determineOverloadSignature("java.lang.Long", "setValue"), 
                "public void setValue(java.lang.Integer ");
        
        // Test unsupported type
        Assert.assertNull(CodeTransformationUtils.determineOverloadSignature("String", "setValue"));
    }

    @Test
    public void testGenerateOverloadedSetter() {
        // Test primitive int field with regular setter
        StringBuilder setter1 = CodeTransformationUtils.generateOverloadedSetter(
                "setCount", "count", "int", false, null);
        Assert.assertTrue(setter1.toString().contains("public void setCount(long value)"));
        Assert.assertTrue(setter1.toString().contains("if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE)"));
        Assert.assertTrue(setter1.toString().contains("this.count = (int) value"));
        
        // Test primitive long field with builder method
        StringBuilder setter2 = CodeTransformationUtils.generateOverloadedSetter(
                "withSize", "size", "long", true, "Builder");
        Assert.assertTrue(setter2.toString().contains("public Builder withSize(int value)"));
        Assert.assertTrue(setter2.toString().contains("return withSize((long) value)"));
        
        // Test wrapper Integer field with regular setter
        StringBuilder setter3 = CodeTransformationUtils.generateOverloadedSetter(
                "setCount", "count", "java.lang.Integer", false, null);
        Assert.assertTrue(setter3.toString().contains("public void setCount(java.lang.Long value)"));
        Assert.assertTrue(setter3.toString().contains("if (value == null)"));
        Assert.assertTrue(setter3.toString().contains("this.count = null"));
        Assert.assertTrue(setter3.toString().contains("this.count = value.intValue()"));
        
        // Test wrapper Long field with builder method
        StringBuilder setter4 = CodeTransformationUtils.generateOverloadedSetter(
                "withSize", "size", "java.lang.Long", true, "Builder");
        Assert.assertTrue(setter4.toString().contains("public Builder withSize(java.lang.Integer value)"));
        Assert.assertTrue(setter4.toString().contains("if (value == null)"));
        Assert.assertTrue(setter4.toString().contains("return withSize((java.lang.Long) null)"));
        Assert.assertTrue(setter4.toString().contains("return withSize(value.longValue())"));
        
        // Test unsupported type (should return empty StringBuilder)
        StringBuilder setter5 = CodeTransformationUtils.generateOverloadedSetter(
                "setName", "name", "String", false, null);
        Assert.assertEquals(setter5.length(), 0);
    }
}
