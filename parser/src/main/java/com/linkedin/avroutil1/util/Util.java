/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.util;

import com.linkedin.avroutil1.model.TextLocation;
import jakarta.json.stream.JsonLocation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

public class Util {

    public static String read(File file) throws IOException {
        try (
                FileInputStream is = new FileInputStream(file);
                InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)
        ) {
            StringWriter writer = new StringWriter();
            char[] buffer = new char[10 * 1024];
            int chars = reader.read(buffer);
            while (chars >= 0) {
                writer.write(buffer, 0, chars);
                chars = reader.read(buffer);
            }
            return writer.toString();
        }
    }

    public static TextLocation convertLocation (JsonLocation jsonLocation) {
        return new TextLocation(jsonLocation.getLineNumber(), jsonLocation.getColumnNumber(), jsonLocation.getStreamOffset());
    }

    public static Throwable rootCause(Throwable throwable) {
        if (throwable == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        Throwable cause = throwable;
        while (cause != null && cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
