/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import com.linkedin.avroutil1.model.AvroType;
import com.linkedin.avroutil1.model.CodeLocation;

import java.util.List;

/**
 * utility class to centralize all issue-creation in a single place
 */
public class AvscIssues {

    private AvscIssues() {
        //utility class
    }

    /**
     * indicates a schema using a "full name" (name property with dots in it) instead of name + namespace
     * @param nameLocation location of the offending name=value pair
     * @param offendingSchemaType type of offending schema
     * @param nameValue offending value of name
     * @return an issue
     */
    public static AvscIssue useOfFullName(CodeLocation nameLocation, AvroType offendingSchemaType, String nameValue) {
        String idealName = nameValue.substring(nameValue.lastIndexOf('.') + 1);
        String idealNamespace = nameValue.substring(0, nameValue.lastIndexOf('.'));
        return new AvscIssue(
                nameLocation,
                IssueSeverity.WARNING,
                offendingSchemaType + " " + nameValue + " is using a full name value instead of "
                        + "setting name=" +idealName + " and namespace=" + idealNamespace,
                null
        );
    }

    /**
     * indicates a schema using a namespace that will be ignored because its also using a full name
     * @param namespaceLocation location of the offending name=value pair
     * @param offendingSchemaType type of offending schema
     * @param nameValue offending value of name
     * @return an issue
     */
    public static AvscIssue ignoredNamespace(
            CodeLocation namespaceLocation,
            AvroType offendingSchemaType,
            String namespaceValue,
            String nameValue
    ) {
        return new AvscIssue(
                namespaceLocation,
                IssueSeverity.WARNING,
                offendingSchemaType + " " + nameValue + " specifies a namespace value (" + namespaceValue + ")"
                        + " at " + namespaceLocation.getStart() + " that will be ignored because its name (" +nameValue
                        + ") already includes a namespace",
                null
        );
    }

    public static AvscIssue badFieldDefaultValue(
            CodeLocation valueLocation,
            String badValue,
            AvroType expectedAvroType,
            String fieldName
    ) {
        return new AvscIssue(
                valueLocation,
                IssueSeverity.SEVERE,
                "default value for field " + fieldName + " at " + valueLocation.getStart()
                        + " cannot be decoded as a " + expectedAvroType + ": " + badValue,
                null
        );
    }

    public static AvscIssue badEnumDefaultValue(
            CodeLocation valueLocation,
            String badValue,
            String enumName,
            List<String> symbols
    ) {
        return new AvscIssue(
                valueLocation,
                IssueSeverity.SEVERE,
                "enum " + enumName + " has a default value of \"" + badValue
                        + "\" at " + valueLocation.getStart() + " which is not in its list of symbols"
                        + " (" + symbols + ")",
                null
        );
    }
}
