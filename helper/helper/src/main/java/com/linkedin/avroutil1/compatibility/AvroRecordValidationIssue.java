/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import com.linkedin.avroutil1.compatibility.avropath.AvroPath;

public class AvroRecordValidationIssue {
    private AvroPath path;
    private String msg;
    private Severity severity;

    public AvroRecordValidationIssue(String msg) {
        this(msg, Severity.ERROR);
    }

    public AvroRecordValidationIssue(String msg, Severity severity) {
        this.msg = msg;
        this.severity = severity;
    }

    public AvroPath getPath() {
        return path;
    }

    //package-private ON PURPOSE
    protected void setPath(AvroPath path) {
        this.path = path;
    }

    public String getMsg() {
        return msg;
    }

    public Severity getSeverity() {
        return severity;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(severity).append("]");
        if (path != null) {
            sb.append(" ").append(path);
        }
        sb.append(": ").append(msg);
        return sb.toString();
    }
}
