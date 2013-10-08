package org.infinispan.persistence;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Test if keys are properly passivated and reloaded in local mode with compatibility enabled
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "persistence.LocalModeCompatibilityPassivationTest")
@CleanupAfterMethod
public class LocalModeCompatibilityPassivationTest extends LocalModePassivationTest {
   @Override
   protected void configureConfiguration(ConfigurationBuilder cb) {
      cb.compatibility().enable();
   }
}
