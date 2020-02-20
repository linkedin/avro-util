/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemplateUtil {
    private static final Pattern TEMPLATE_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(\\w+)}");

    public static String loadTemplate(String templateName) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(templateName)) {
            if (is == null) {
                throw new IllegalStateException("unable to find " + templateName);
            }
            try (InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
                 BufferedReader bufferedReader = new BufferedReader(reader)) {
                StringWriter writer = new StringWriter();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    writer.append(line).append(System.lineSeparator());
                }
                writer.flush();
                return writer.toString();
            }
        } catch (Exception e) {
            throw new IllegalStateException("unable to load template " + templateName, e);
        }
    }

    public static String populateTemplate(String template, Map<String, String> parameters) {
        //poor-man's regexp-based templating engine
        Matcher paramMatcher = TEMPLATE_PLACEHOLDER_PATTERN.matcher(template);
        StringBuffer output = new StringBuffer();
        while (paramMatcher.find()) {
            String paramName = paramMatcher.group(1);
            String paramValue = parameters.get(paramName);
            if (paramValue == null) {
                throw new IllegalStateException("parameters have no value for " + paramName);
            }
            paramMatcher.appendReplacement(output, paramValue);
        }
        paramMatcher.appendTail(output);
        return output.toString();
    }
}
