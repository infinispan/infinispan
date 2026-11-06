package org.infinispan.server.test.core.compatibility;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TestFailure(
      @JsonProperty("test-class") String testClass,
      @JsonProperty("test-methods") List<String> testMethods
) {
}
