/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.spotbugs;

import com.linkedin.avroutil1.TestUtil;
import edu.umd.cs.findbugs.BugCollection;
import edu.umd.cs.findbugs.test.SpotBugsRule;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcher;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcherBuilder;
import org.hamcrest.MatcherAssert;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static edu.umd.cs.findbugs.test.CountMatcher.containsExactly;

public class OldSchemaConstructableUsageDetectorTest {
    @Rule
    public SpotBugsRule spotbugs = new SpotBugsRule();

    //only compiles under avro < 1.6
    @Test
    public void testBadCase() throws Exception {
        List<Path> classFiles = TestUtil.findClassFiles("build/classes/java/test", BadClass.class);
        BugCollection bugCollection = spotbugs.performAnalysis(classFiles.toArray(new Path[0]));

        BugInstanceMatcher bugTypeMatcher = new BugInstanceMatcherBuilder()
                .bugType("OLD_SCHEMACONSTRUCTABLE_USAGE")
                .inMethod("useOldSchemaConstructable").build();
        //some of our bugs get deduped
        MatcherAssert.assertThat(bugCollection, containsExactly(2, bugTypeMatcher));

        bugTypeMatcher = new BugInstanceMatcherBuilder()
                .bugType("OLD_SCHEMACONSTRUCTABLE_USAGE")
                .inClass(BadClass.OldSchemaConstructable.class.getName()).build();
        MatcherAssert.assertThat(bugCollection, containsExactly(1, bugTypeMatcher));

        bugTypeMatcher = new BugInstanceMatcherBuilder()
                .bugType("OLD_SCHEMACONSTRUCTABLE_USAGE")
                .inMethod("useOldSchemaConstructableSomeMore").build();
        MatcherAssert.assertThat(bugCollection, containsExactly(1, bugTypeMatcher));
    }

    @Test
    public void testGoodCase() {
        Path path = Paths.get("build/classes/java/test", GoodClass.class.getName().replace('.', '/') + ".class");
        BugCollection bugCollection = spotbugs.performAnalysis(path);

        BugInstanceMatcher bugTypeMatcher = new BugInstanceMatcherBuilder()
                .bugType("OLD_SCHEMACONSTRUCTABLE_USAGE")
                .build();
        MatcherAssert.assertThat(bugCollection, containsExactly(0, bugTypeMatcher));
    }
}
