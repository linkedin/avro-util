package com.linkedin.avro.codegen;

import java.util.Map;
import java.util.Set;

class ClasspathFishingResults {
    private final Set<String> fqcnsNotFound;
    private final Map<String, String> fqcnsFoundButUnusable;
    private final Map<String, SchemaDetails> fqcnsFound;

    public ClasspathFishingResults(
            Set<String> fqcnsNotFound,
            Map<String, String> fqcnsFoundButUnusable,
            Map<String, SchemaDetails> fqcnsFound
    ) {
        this.fqcnsNotFound = fqcnsNotFound;
        this.fqcnsFoundButUnusable = fqcnsFoundButUnusable;
        this.fqcnsFound = fqcnsFound;
    }

    public Set<String> getFqcnsNotFound() {
        return fqcnsNotFound;
    }

    public Map<String, String> getFqcnsFoundButUnusable() {
        return fqcnsFoundButUnusable;
    }

    public Map<String, SchemaDetails> getFqcnsFound() {
        return fqcnsFound;
    }
}
