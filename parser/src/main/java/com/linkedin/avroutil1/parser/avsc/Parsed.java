/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser.avsc;

import java.util.ArrayList;
import java.util.List;

/**
 * represents the result of parsing some part of the schema.
 * composed of the thing that was parsed and any issued found
 * while parsing it.
 * note that the 2 values are not mutually exclusive - it's possible to parse
 * something and also record issues (obviously not fatal ones) during.
 * @param <T> the type of thing that was (maybe) parsed.
 */
public class Parsed<T> {
    private T data;
    private List<AvscIssue> issues;

    public Parsed() {
        this.data = null;
        this.issues = null;
    }

    public Parsed(T data) {
        //we allow data to be null
        this.data = data;
        this.issues = null;
    }

    public Parsed(AvscIssue issue) {
        if (issue == null) {
            throw new IllegalArgumentException("issue cannot be null");
        }
        this.data = null;
        this.issues = new ArrayList<>(1);
        this.issues.add(issue);
    }

    public boolean hasData() {
        return data != null;
    }

    public T getData() {
        return data;
    }

    public boolean hasIssues() {
        return issues != null && !issues.isEmpty();
    }

    public List<AvscIssue> getIssues() {
        return issues;
    }

    public void setData(T data) {
        if (this.data != null) {
            throw new IllegalStateException("data has already been set to " + this.data + " so cannot be changed to " + data);
        }
        this.data = data;
    }

    public void recordIssue(AvscIssue issue) {
        if (issues == null) {
            issues = new ArrayList<>(1);
        }
        issues.add(issue);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean hasData = hasData();
        if (hasData) {
            sb.append(getData());
        }
        if (hasIssues()) {
            if (hasData) {
                sb.append(" (and ");
            }
            sb.append(issues.size()).append(" issues");
            if (hasData) {
                sb.append(")");
            }
        }
        return sb.toString();
    }
}
