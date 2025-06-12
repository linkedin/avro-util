/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

public class CodeTransformationUtils {
    private CodeTransformationUtils() {
        //util class
    }

    /**
     * Constants for Java numeric type names that are used in multiple places
     */
    public static final String JAVA_LANG_INTEGER = "java.lang.Integer";
    public static final String JAVA_LANG_LONG = "java.lang.Long";
    
    /**
     * Finds the index position of the end of a code block by counting braces.
     * Used to find the end of methods, classes, etc.
     *
     * @param code The source code string
     * @param startPosition The position from which to start looking (usually after the opening brace)
     * @return The position of the end of the block (after the closing brace) or -1 if the block end can't be found
     */
    public static int findEndOfBlock(String code, int startPosition) {
        if (startPosition < 0 || startPosition >= code.length()) {
            return -1; // Invalid start position
        }
        
        // If we're not starting at a brace, find the first opening brace
        int position = startPosition;
        if (code.charAt(position) != '{') {
            position = code.indexOf('{', position);
            if (position == -1) {
                return -1; // No opening brace found
            }
        }
        
        // Start counting from after the opening brace
        position++;
        
        int braceCount = 1; // We've already seen the opening brace
        while (position < code.length() && braceCount > 0) {
            char c = code.charAt(position);
            if (c == '{') {
                braceCount++;
            } else if (c == '}') {
                braceCount--;
            }
            position++;
        }
        
        // If brace count is not 0, we didn't find the matching closing brace
        return braceCount == 0 ? position : -1;
    }
    
    /**
     * Generates Javadoc for an overloaded numeric setter method or builder setter method.
     *
     * @param fieldName Name of the field being modified
     * @param sourceType The type of the parameter (e.g. "long" or "java.lang.Long")
     * @param targetType The type of the field (e.g. "int" or "java.lang.Integer")
     * @param isBuilderMethod Whether this is for a builder method (returns builder) or a regular setter (returns void)
     * @return StringBuilder containing the generated Javadoc
     */
    public static StringBuilder generateNumericConversionJavadoc(String fieldName, String sourceType, String targetType, boolean isBuilderMethod) {
        StringBuilder javadoc = new StringBuilder("\n\n    /**\n");
        javadoc.append("     * Sets ").append(fieldName).append(" to the specified value.\n");
        
        // Add description based on source and target types
        if ("int".equals(sourceType) || JAVA_LANG_INTEGER.equals(sourceType)) {
            javadoc.append("     * Accepts an ");
            javadoc.append("int".equals(sourceType) ? "int" : "Integer");
            javadoc.append(" value and converts it to ");
            javadoc.append("long".equals(targetType) ? "long" : "Long");
            javadoc.append(".\n");
        } else if ("long".equals(sourceType) || JAVA_LANG_LONG.equals(sourceType)) {
            javadoc.append("     * Accepts a ");
            javadoc.append("long".equals(sourceType) ? "long" : "Long");
            javadoc.append(" value and converts it to ");
            javadoc.append("int".equals(targetType) ? "int" : "Integer");
            javadoc.append(" with bounds checking.\n");
        }
        
        // Add parameter description
        javadoc.append("     * @param value The ");
        javadoc.append("int".equals(sourceType) ? "int" : 
                       "long".equals(sourceType) ? "long" : 
                       JAVA_LANG_INTEGER.equals(sourceType) ? "Integer" : "Long");
        javadoc.append(" value to set\n");
        
        // Add return value description for builder methods
        if (isBuilderMethod) {
            javadoc.append("     * @return This builder\n");
        }
        
        // Add exception for bounds checking (int/Integer target types)
        if ("int".equals(targetType) || JAVA_LANG_INTEGER.equals(targetType)) {
            javadoc.append("     * @throws org.apache.avro.AvroRuntimeException if the value is outside the ");
            javadoc.append("int".equals(targetType) ? "int" : "Integer");
            javadoc.append(" range\n");
        }
        
        javadoc.append("     */\n");
        return javadoc;
    }
    
    /**
     * Generates the method signature for an overloaded numeric setter or builder setter.
     *
     * @param methodName The name of the method (e.g., "setIntField")
     * @param sourceType The source type of the parameter (e.g., "long" or "java.lang.Long")
     * @param paramName The name of the parameter (usually "value")
     * @param returnType The return type, "void" for regular setters or builder class name for builder setters
     * @return StringBuilder containing the generated method signature including the opening brace
     */
    public static StringBuilder generateNumericMethodSignature(
            String methodName, String sourceType, String paramName, String returnType) {
        StringBuilder signature = new StringBuilder("    public ");
        signature.append(returnType).append(" ")
                .append(methodName).append("(")
                .append(sourceType).append(" ")
                .append(paramName).append(") {\n");
        return signature;
    }
    
    /**
     * Generates code for converting between numeric types with appropriate bounds checking and null handling.
     * 
     * @param fieldName The field name to set
     * @param sourceType The source type parameter (e.g., "long", "java.lang.Long")
     * @param targetType The target type field (e.g., "int", "java.lang.Integer")
     * @param isBuilderMethod Whether this is for a builder method (returns the builder) or regular setter
     * @param builderMethodName For builder methods, the name of the method to return to (usually same as methodName)
     * @return StringBuilder containing the generated method body without the closing brace
     */
    public static StringBuilder generateNumericConversionCode(
            String fieldName, String sourceType, String targetType, 
            boolean isBuilderMethod, String builderMethodName) {
        StringBuilder code = new StringBuilder();
        
        if ("int".equals(targetType) && "long".equals(sourceType)) {
            // Convert long to int with bounds check
            code.append("        if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {\n");
            if (isBuilderMethod) {
                code.append("            return ").append(builderMethodName).append("((int) value);\n");
            } else {
                code.append("            this.").append(fieldName).append(" = (int) value;\n");
            }
            code.append("        } else {\n");
            code.append("            throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to int\");\n");
            code.append("        }");
        } else if ("long".equals(targetType) && "int".equals(sourceType)) {
            // Convert int to long
            if (isBuilderMethod) {
                code.append("        return ").append(builderMethodName).append("((long) value);");
            } else {
                code.append("        this.").append(fieldName).append(" = (long) value;");
            }
        } else if (JAVA_LANG_INTEGER.equals(targetType) && JAVA_LANG_LONG.equals(sourceType)) {
            // Convert Long to Integer with bounds check and null handling
            code.append("        if (value == null) {\n");
            if (isBuilderMethod) {
                code.append("            return ").append(builderMethodName).append("((").append(JAVA_LANG_INTEGER).append(") null);\n");
            } else {
                code.append("            this.").append(fieldName).append(" = null;\n");
            }
            code.append("        } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {\n");
            if (isBuilderMethod) {
                code.append("            return ").append(builderMethodName).append("(value.intValue());\n");
            } else {
                code.append("            this.").append(fieldName).append(" = value.intValue();\n");
            }
            code.append("        } else {\n");
            code.append("            throw new org.apache.avro.AvroRuntimeException(\"Long value \" + value + \" cannot be cast to Integer\");\n");
            code.append("        }");
        } else if (JAVA_LANG_LONG.equals(targetType) && JAVA_LANG_INTEGER.equals(sourceType)) {
            // Convert Integer to Long with null handling
            code.append("        if (value == null) {\n");
            if (isBuilderMethod) {
                code.append("            return ").append(builderMethodName).append("((").append(JAVA_LANG_LONG).append(") null);\n");
            } else {
                code.append("            this.").append(fieldName).append(" = null;\n");
            }
            code.append("        } else {\n");
            if (isBuilderMethod) {
                code.append("            return ").append(builderMethodName).append("(value.longValue());\n");
            } else {
                code.append("            this.").append(fieldName).append(" = value.longValue();\n");
            }
            code.append("        }");
        }
        
        return code;
    }
    
    /**
     * Determines the appropriate overloaded method signature for a given field type.
     * 
     * @param fieldType The type of the field (e.g., "int", "java.lang.Integer")
     * @param methodName The setter method name
     * @return The signature of the overloaded method, or null if no overloaded method is needed
     */
    public static String determineOverloadSignature(String fieldType, String methodName) {
        String overloadSignature = null;
        if ("int".equals(fieldType)) {
            overloadSignature = "public void " + methodName + "(long ";
        } else if ("long".equals(fieldType)) {
            overloadSignature = "public void " + methodName + "(int ";
        } else if (JAVA_LANG_INTEGER.equals(fieldType)) {
            overloadSignature = "public void " + methodName + "(java.lang.Long ";
        } else if (JAVA_LANG_LONG.equals(fieldType)) {
            overloadSignature = "public void " + methodName + "(java.lang.Integer ";
        }
        return overloadSignature;
    }
    
    /**
     * Generates the complete code for an overloaded setter method with proper type conversion.
     *
     * @param methodName The name of the setter method
     * @param fieldName The name of the field being set
     * @param fieldType The type of the field (e.g., "int", "java.lang.Integer")
     * @param isBuilderMethod Whether this is for a builder method
     * @param builderReturnType If isBuilderMethod is true, the return type for the builder method
     * @return Complete code for the overloaded setter method
     */
    public static StringBuilder generateOverloadedSetter(
            String methodName, String fieldName, String fieldType,
            boolean isBuilderMethod, String builderReturnType) {
        
        StringBuilder overloadedSetter = new StringBuilder();
        String returnType = isBuilderMethod ? builderReturnType : "void";
        String sourceType;
        String targetType = fieldType;
        
        // Determine the appropriate source type based on the target field type
        if ("int".equals(fieldType)) {
            sourceType = "long";
        } else if ("long".equals(fieldType)) {
            sourceType = "int";
        } else if (JAVA_LANG_INTEGER.equals(fieldType)) {
            sourceType = JAVA_LANG_LONG;
        } else if (JAVA_LANG_LONG.equals(fieldType)) {
            sourceType = JAVA_LANG_INTEGER;
        } else {
            return overloadedSetter; // Empty if not a numeric type we handle
        }
        
        // Generate the method Javadoc
        overloadedSetter.append(generateNumericConversionJavadoc(fieldName, sourceType, targetType, isBuilderMethod));
        
        // Generate the method signature
        overloadedSetter.append(generateNumericMethodSignature(methodName, sourceType, "value", returnType));
        
        // Generate the conversion code
        overloadedSetter.append(generateNumericConversionCode(fieldName, sourceType, targetType, isBuilderMethod, methodName));
        
        // Close the method
        overloadedSetter.append("\n    }");
        
        return overloadedSetter;
    }
}
