/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import edu.umd.cs.findbugs.BugCollection;
import edu.umd.cs.findbugs.test.SpotBugsRule;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcher;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcherBuilder;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static edu.umd.cs.findbugs.test.CountMatcher.containsExactly;

public class JsonDecoderInstantiationDetectorTest {
    @Rule
    public SpotBugsRule spotbugs = new SpotBugsRule();

    @Test
    public void testBadCase() {
        Path path = Paths.get("build/classes/java/test", BadClass.class.getName().replace('.', '/') + ".class");
        BugCollection bugCollection = spotbugs.performAnalysis(path);

        BugInstanceMatcher bugTypeMatcher = new BugInstanceMatcherBuilder()
                .bugType("JSON_DECODER_INSTANTIATION")
                .inMethod("instantiateJsonDecoder").build();
        MatcherAssert.assertThat(bugCollection, containsExactly(2, bugTypeMatcher));
    }

    @Test
    public void testGoodCase() {
        Path path = Paths.get("build/classes/java/test", GoodClass.class.getName().replace('.', '/') + ".class");
        BugCollection bugCollection = spotbugs.performAnalysis(path);

        BugInstanceMatcher bugTypeMatcher = new BugInstanceMatcherBuilder()
                .bugType("JSON_DECODER_INSTANTIATION")
                .build();
        MatcherAssert.assertThat(bugCollection, containsExactly(0, bugTypeMatcher));
    }
}
