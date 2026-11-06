package org.infinispan.server.test.core.compatibility;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ExceptionDetail(
    String description,
    String issue,
    @JsonProperty("test-failures") List<TestFailure> testFailures
) {}
