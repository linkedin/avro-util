/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility;

import java.util.Random;

/**
 * configuration for use with {@link RandomRecordGenerator}
 */
public class RecordGenerationConfig {
    private final long seed;
    private final Random random;

    private final Random randomToUse;

    public RecordGenerationConfig(long seed, Random random) {
        this.seed = seed;
        this.random = random;

        this.randomToUse = this.random != null ? this.random : new Random(this.seed);
    }

    public RecordGenerationConfig(RecordGenerationConfig toCopy) {
        if (toCopy == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        this.seed = toCopy.seed;
        this.random = toCopy.random;

        this.randomToUse = this.random != null ? this.random : new Random(this.seed);
    }

    public Random random() {
        return randomToUse;
    }

    // "builder-style" copy-setters

    public static RecordGenerationConfig newConfig() {
        //we provide seed and not Random so that copies of this config get their own random
        //instances. this is done so the defaults produce consistent behaviour even under MT use
        return new RecordGenerationConfig(System.currentTimeMillis(), null);
    }

    public RecordGenerationConfig withSeed(long seed) {
        return new RecordGenerationConfig(
                seed,
                this.random
        );
    }

    public RecordGenerationConfig withRandom(Random random) {
        return new RecordGenerationConfig(
                this.seed,
                random
        );
    }

}
