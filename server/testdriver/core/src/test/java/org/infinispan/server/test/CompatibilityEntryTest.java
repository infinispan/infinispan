package org.infinispan.server.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.infinispan.server.test.core.compatibility.Compatibility;
import org.infinispan.server.test.core.compatibility.CompatibilityEntry;
import org.infinispan.server.test.core.compatibility.ExceptionDetail;
import org.infinispan.server.test.core.compatibility.TestFailure;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeVersion;
import org.junit.jupiter.api.Test;

public class CompatibilityEntryTest {

   @Test
   public void testMatchesVersions() {
      CompatibilityEntry entry = new CompatibilityEntry("[16.0,16.1)", "[16.1,)", Map.of(), List.of());
      assertTrue(entry.matchesVersions("16.0.4", "16.1.1"));
      assertTrue(entry.matchesVersions("16.0.4", "16.2.0"));
      assertFalse(entry.matchesVersions("16.0.4", "16.0.5"));
   }

   @Test
   public void testMethodLevelSkip() {
      Compatibility compatibility = compatibility(new TestFailure("com.example.OtherTests", List.of("testOne")));

      assertTrue(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.OtherTests", "testOne"));
      assertFalse(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.OtherTests", "testTwo"));
      assertFalse(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.Unlisted", "testOne"));
   }

   @Test
   public void testClassLevelSkipWithoutTestMethods() {
      // a missing test-methods list excludes every method of the class
      Compatibility compatibility = compatibility(new TestFailure("com.example.ExpressionTests", null));

      assertTrue(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.ExpressionTests", "testAnything"));
      assertTrue(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.ExpressionTests", "testSomethingElse"));

      // outside the excluded version range nothing is skipped
      assertFalse(compatibility.isCompatibilitySkip(configuration("16.1.0", "16.2.0"), "com.example.ExpressionTests", "testAnything"));
   }

   @Test
   public void testClassLevelSkipWithEmptyTestMethods() {
      // an empty test-methods list behaves like a missing one
      Compatibility compatibility = compatibility(new TestFailure("com.example.ExpressionTests", List.of()));

      assertTrue(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.ExpressionTests", "testAnything"));
   }

   @Test
   public void testRegexMethodSkip() {
      // each entry is a regular expression matched against the whole method name
      Compatibility compatibility = compatibility(new TestFailure("com.example.ExpressionTests",
            List.of("testSelect.*", "testUpdateStatementWith.*")));

      assertTrue(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.ExpressionTests", "testSelectFunctionCallConcat"));
      assertTrue(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.ExpressionTests", "testUpdateStatementWithDivideFunction"));
      assertFalse(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.ExpressionTests", "testAttributeQuery"));
      assertFalse(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.OtherTests", "testSelectFunctionCallConcat"));

      // the regex exclusion is still scoped to the entry's version range
      assertFalse(compatibility.isCompatibilitySkip(configuration("16.1.0", "16.2.0"), "com.example.ExpressionTests", "testSelectFunctionCallConcat"));
   }

   @Test
   public void testInvalidRegexIsTreatedAsLiteral() {
      // an invalid pattern must not crash the matcher: it is treated as a literal method name
      Compatibility compatibility = compatibility(new TestFailure("com.example.Broken", List.of("test(One")));

      assertFalse(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.Broken", "testOne"));
      assertTrue(compatibility.isCompatibilitySkip(configuration("16.0.4", "16.1.1"), "com.example.Broken", "test(One"));
   }

   private static Compatibility compatibility(TestFailure... testFailures) {
      ExceptionDetail detail = new ExceptionDetail("description", "https://example.com/issue", List.of(testFailures));
      CompatibilityEntry entry = new CompatibilityEntry("[16.0,16.1)", "[16.1,)", Map.of(), List.of(detail));
      return new Compatibility(List.of(entry));
   }

   private static RollingUpgradeConfiguration configuration(String fromVersion, String toVersion) {
      return new RollingUpgradeConfiguration(0,
            new RollingUpgradeVersion(null, fromVersion),
            new RollingUpgradeVersion(null, toVersion),
            null, null, 0, false, null, false, null, null, null, null, null, null, null, null);
   }
}
