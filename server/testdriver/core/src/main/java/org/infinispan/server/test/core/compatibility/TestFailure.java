package org.infinispan.server.test.core.compatibility;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A test exclusion: the listed methods of the class, or every method of the
 * class when {@code test-methods} is omitted or empty (a whole-class
 * exclusion, for test classes dedicated to functionality that is not
 * available on older versions).
 * <p>
 * Each entry in {@code test-methods} is a regular expression matched against
 * the whole method name, so a plain method name matches itself while patterns
 * such as {@code testSelect.*} exclude a family of methods. The exclusion
 * applies only within the version range of the enclosing entry.
 */
public record TestFailure(
      @JsonProperty("test-class") String testClass,
      @JsonProperty("test-methods") List<String> testMethods
) {
   public TestFailure {
      if (testMethods == null) testMethods = List.of();
   }
}
