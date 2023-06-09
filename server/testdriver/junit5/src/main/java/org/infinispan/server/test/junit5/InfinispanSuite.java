package org.infinispan.server.test.junit5;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Hack to ensure that {@link InfinispanServerExtension} instances can be shared against test classes executed across a
 * {@link org.junit.platform.suite.api.Suite}. A no-op test is required in the Suite class to ensure that calls to
 * {@link org.junit.jupiter.api.extension.RegisterExtension} are respected. The {@link InfinispanServerExtension} can
 * then register the {@link org.junit.platform.suite.api.SelectClasses} values so that the server resources are only
 * cleaned up on the final call to {@link org.junit.jupiter.api.extension.AfterAllCallback}.
 * <p>
 * All test Suites requiring {@link InfinispanServerExtension} should extend this class.
 * <p>
 * This can be removed when JUnit provides native support for <a href="https://github.com/junit-team/junit5/issues/456">@BeforeSuite and @AfterSuite annotations</a>.
 */
public abstract class InfinispanSuite {
   @Disabled("No-op test to ensure InfinispanServerExtension at the suite level is registered")
   @DisplayName("Suite Initializer")
   @Test()
   public void ignore() {
      // no-op
   }
}
